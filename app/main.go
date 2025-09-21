package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	ECHO   = "ECHO"
	PING   = "PING"
	GET    = "GET"
	SET    = "SET"
	RPUSH  = "RPUSH"
	LRANGE = "LRANGE"
	LPUSH  = "LPUSH"
	LLEN   = "LLEN"
	LPOP   = "LPOP"
	BLPOP  = "BLPOP"
)

type respStringType string

const (
	BULK    respStringType = "BULK"
	SIMPLE  respStringType = "SIMPLE"
	INTEGER respStringType = "INTEGER"
	ARRAY   respStringType = "ARRAY"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type StoreValue struct {
	value     string
	expiresAt time.Time
}

type Store struct {
	data            map[string]StoreValue
	lists           map[string][]string
	mutList         map[string]*sync.RWMutex
	blockedChannels map[string][]chan string
}

func NewStore() *Store {
	return &Store{
		data:            make(map[string]StoreValue),
		lists:           make(map[string][]string),
		mutList:         make(map[string]*sync.RWMutex),
		blockedChannels: make(map[string][]chan string),
	}
}

func (s *Store) Set(key string, value StoreValue) {
	s.data[key] = StoreValue{value: value.value, expiresAt: value.expiresAt}
}

func (s *Store) Get(key string) (StoreValue, bool) {
	val, ok := s.data[key]
	return val, ok
}

func (s *Store) GetMutex(key string) *sync.RWMutex {
	if _, ok := s.mutList[key]; !ok {
		s.mutList[key] = &sync.RWMutex{}
	}
	return s.mutList[key]
}

func (s *Store) Rpush(key string, value []string) {
	val, ok := s.lists[key]
	mutex := s.GetMutex(key)
	mutex.Lock()
	defer mutex.Unlock()
	if ok {
		s.lists[key] = append(val, value...)
	} else {
		s.lists[key] = value
	}
}

func (s *Store) Lpush(key string, value []string) {
	val, ok := s.lists[key]
	mutex := s.GetMutex(key)
	mutex.Lock()
	defer mutex.Unlock()
	if ok {
		slices.Reverse(value)
		s.lists[key] = append(value, val...)
	} else {
		s.lists[key] = value
	}
}

func (s *Store) LRange(key string, start, stop int) []string {
	mutex := s.GetMutex(key)
	mutex.RLock()
	defer mutex.RUnlock()
	if start >= len(s.lists[key]) {
		return []string{}
	}
	return s.lists[key][start:min(stop+1, len(s.lists[key]))]
}

func (s *Store) LPop(key string) string {
	mutex := s.GetMutex(key)
	mutex.Lock()
	defer mutex.Unlock()
	val, ok := s.lists[key]
	if !ok {
		return ""
	} else {
		value := val[0]
		s.lists[key] = val[1:]
		return value
	}
}

func (s *Store) LPopMultiple(key string, num int) []string {
	mutex := s.GetMutex(key)
	mutex.Lock()
	defer mutex.Unlock()
	val, ok := s.lists[key]
	if !ok {
		return []string{}
	} else {
		values := val[:num]
		s.lists[key] = val[num:]
		return values
	}
}

var GlobalStore = NewStore()

func handleBlpop(conn net.Conn, key, wait string) error {
	waitTime, err := strconv.Atoi(wait)
	if err != nil {
		return err
	}
	if len(GlobalStore.lists[key]) > 0 {
		val := GlobalStore.LPop(key)
		return respArray(conn, []string{key, val})
	}
	if waitTime == 0 {
		ch := make(chan string, 1)
		defer close(ch)
		GlobalStore.blockedChannels[key] = append(GlobalStore.blockedChannels[key], ch)
		val := <-ch
		return respArray(conn, []string{key, val})
	}
	timeout := time.After(time.Duration(waitTime) * time.Second)
	ch := make(chan string, 1)
	defer close(ch)
	GlobalStore.blockedChannels[key] = append(GlobalStore.blockedChannels[key], ch)

	select {
	case val := <-ch:
		return respArray(conn, []string{key, val})
	case <-timeout:
		_, err := conn.Write([]byte("$-1\r\n"))
		return err
	}
}

func handleLRange(conn net.Conn, key, l, r string) error {
	left, err := strconv.Atoi(l)
	if err != nil {
		return err
	}
	right, err := strconv.Atoi(r)
	if err != nil {
		return err
	}
	n := len(GlobalStore.lists[key])
	if left < 0 {
		left += n
	}
	left = max(left, 0)
	for right < 0 {
		right += n
	}
	right = min(right, n-1)
	elem := GlobalStore.LRange(key, left, right)
	return respArray(conn, elem)
}

func handleRpush(conn net.Conn, key string, value []string) error {
	GlobalStore.Rpush(key, value)
	length := len(GlobalStore.lists[key])
	for _, channel := range GlobalStore.blockedChannels[key] {
		if len(value) == 0 {
			break
		}
		channel <- value[0]
		value = value[1:]
		GlobalStore.blockedChannels[key] = GlobalStore.blockedChannels[key][1:]
	}
	return respWriter(conn, INTEGER, strconv.Itoa(length))
}

func handleLPush(conn net.Conn, key string, value []string) error {
	GlobalStore.Lpush(key, value)
	length := len(GlobalStore.lists[key])
	for _, channel := range GlobalStore.blockedChannels[key] {
		if len(value) == 0 {
			break
		}
		// last element was first inserted in case of Lpush
		channel <- value[len(value)-1]
		value = value[:len(value)-1]
		GlobalStore.blockedChannels[key] = GlobalStore.blockedChannels[key][1:]
	}
	return respWriter(conn, INTEGER, strconv.Itoa(length))
}

func handleLlen(conn net.Conn, key string) error {
	length := len(GlobalStore.lists[key])
	return respWriter(conn, INTEGER, strconv.Itoa(length))
}

func handleLpop(conn net.Conn, key string) error {
	if len(GlobalStore.lists[key]) == 0 {
		if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
			return err
		}
		return nil
	} else {
		val := GlobalStore.LPop(key)
		return respWriter(conn, BULK, val)
	}
}

func handleLpopMultiple(conn net.Conn, key string, n string) error {
	num, err := strconv.Atoi(n)
	if err != nil {
		return err
	}
	if len(GlobalStore.lists[key]) == 0 {
		if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
			return err
		}
		return nil
	} else if len(GlobalStore.lists[key]) <= num {
		return respArray(conn, GlobalStore.lists[key])
	} else {
		values := GlobalStore.LPopMultiple(key, num)
		return respArray(conn, values)
	}
}

func handleGet(conn net.Conn, key string) error {
	val, ok := GlobalStore.Get(key)
	if ok {
		if time.Now().After(val.expiresAt) {
			delete(GlobalStore.data, key)
			if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
				return err
			}
			return nil
		} else {
			return respWriter(conn, BULK, val.value)
		}
	} else {
		if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
			return err
		}
		return nil
	}
}

func handleSet(conn net.Conn, key, value string, expireDuration time.Duration) error {
	expiresAt := time.Now().Add(expireDuration)
	GlobalStore.Set(key, StoreValue{value: value, expiresAt: expiresAt})
	return respWriter(conn, SIMPLE, "OK")
}

func handleEcho(conn net.Conn, str string) error {
	return respWriter(conn, BULK, str)
}

func handlePing(conn net.Conn) error {
	return respWriter(conn, SIMPLE, "PONG")
}

func respParser(conn net.Conn) ([]string, error) {
	var buf = make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	sep := []byte{'\r', '\n'}
	parts := bytes.Split(buf[:n], sep)
	var args []string
	for i := 2; i < len(parts); i += 2 {
		args = append(args, string(parts[i]))
	}
	return args, nil
}

func respArray(conn net.Conn, a []string) error {
	msg := fmt.Sprintf("*%d\r\n", len(a))
	for _, v := range a {
		msg += fmt.Sprintf("$%d\r\n", len(v))
		msg += fmt.Sprintf("%s\r\n", v)
	}
	if _, err := conn.Write([]byte(msg)); err != nil {
		return err
	}
	return nil
}

func respWriter(conn net.Conn, strType respStringType, str string) error {
	var msg string
	switch strType {
	case BULK:
		msg = fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
	case SIMPLE:
		msg = fmt.Sprintf("+%s\r\n", str)
	case INTEGER:
		msg = fmt.Sprintf(":%s\r\n", str)
	}
	if _, err := conn.Write([]byte(msg)); err != nil {
		return err
	}
	return nil
}

func handleConnection(conn net.Conn) error {
	defer conn.Close()

	for {
		args, err := respParser(conn)
		if err != nil {
			if err != io.EOF {
				return err
			} else {
				fmt.Println("Connection closed")
				return nil
			}
		}
		switch strings.ToUpper(args[0]) {
		case PING:
			if err = handlePing(conn); err != nil {
				return err
			}
		case ECHO:
			if err = handleEcho(conn, args[1]); err != nil {
				return err
			}
		case GET:
			if err = handleGet(conn, args[1]); err != nil {
				return err
			}
		case SET:
			if len(args) != 3 {
				if strings.ToUpper(args[3]) != "PX" {
					return fmt.Errorf("invalid arguments")
				} else {
					expires, err := strconv.Atoi(args[4])
					if err != nil {
						return err
					}
					if err = handleSet(conn, args[1], args[2], time.Duration(expires)*time.Millisecond); err != nil {
						return err
					}
				}
			} else {
				if err = handleSet(conn, args[1], args[2], 24*time.Hour); err != nil {
					return err
				}
			}
		case RPUSH:
			if err = handleRpush(conn, args[1], args[2:]); err != nil {
				return err
			}
		case LRANGE:
			if err = handleLRange(conn, args[1], args[2], args[3]); err != nil {
				return err
			}
		case LPUSH:
			if err = handleLPush(conn, args[1], args[2:]); err != nil {
				return err
			}
		case LLEN:
			if err = handleLlen(conn, args[1]); err != nil {
				return err
			}
		case LPOP:
			if len(args) != 2 {
				if err = handleLpopMultiple(conn, args[1], args[2]); err != nil {
					return err
				}
			} else {
				if err = handleLpop(conn, args[1]); err != nil {
					return err
				}
			}
		case BLPOP:
			if err = handleBlpop(conn, args[1], args[2]); err != nil {
				return err
			}
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go func() {
			err := handleConnection(conn)
			if err != nil {
				fmt.Println("Error handling connection: ", err.Error())
			}
		}()
	}
}
