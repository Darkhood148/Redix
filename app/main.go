package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

const (
	ECHO = "ECHO"
	PING = "PING"
	GET  = "GET"
	SET  = "SET"
)

type respStringType string

const (
	BULK   respStringType = "BULK"
	SIMPLE respStringType = "SIMPLE"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type Store struct {
	data map[string]string
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

func (s *Store) Set(key, value string) {
	s.data[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	val, ok := s.data[key]
	return val, ok
}

var GlobalStore = NewStore()

func handleGet(conn net.Conn, key string) error {
	val, ok := GlobalStore.Get(key)
	if ok {
		respStr, err := respWriter(BULK, val)
		if err != nil {
			return err
		}
		if _, err := conn.Write([]byte(respStr)); err != nil {
			return err
		}
		return nil
	} else {
		if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
			return err
		}
		return nil
	}
}

func handleSet(conn net.Conn, key, value string) error {
	GlobalStore.Set(key, value)
	respStr, err := respWriter(SIMPLE, "OK")
	if err != nil {
		return err
	}
	if _, err := conn.Write([]byte(respStr)); err != nil {
		return err
	}
	return nil
}

func handleEcho(conn net.Conn, str string) error {
	respStr, err := respWriter(BULK, str)
	if err != nil {
		return err
	}
	if _, err := conn.Write([]byte(respStr)); err != nil {
		return err
	}
	return nil
}

func handlePing(conn net.Conn) error {
	respStr, err := respWriter(SIMPLE, "PONG")
	if err != nil {
		return err
	}
	if _, err := conn.Write([]byte(respStr)); err != nil {
		return err
	}
	return nil
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

func respWriter(strType respStringType, str string) (string, error) {
	switch strType {
	case BULK:
		return fmt.Sprintf("$%d\r\n%s\r\n", len(str), str), nil
	case SIMPLE:
		return fmt.Sprintf("+%s\r\n", str), nil
	}
	return "", fmt.Errorf("unknown response type: %s", strType)
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
			if err = handleSet(conn, args[1], args[2]); err != nil {
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
