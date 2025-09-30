package main

import (
	"net"
	"strconv"
	"time"
)

type XRangeSerialized struct {
	id     string
	fields []string
}

type XReadSerialized struct {
	stream  []string
	entries [][]XRangeSerialized
}

func handleXread(conn net.Conn, n int, params []string) error {
	var ans XReadSerialized
	var streams []string
	var ids []string
	for i := 0; i < len(params)/2; i++ {
		streams = append(streams, params[i])
		if params[i+len(params)/2] == "$" {
			x := GlobalStore.streams[streams[i]]
			y := x[len(x)-1].ID
			ids = append(ids, y)
		} else {
			ids = append(ids, params[i+len(params)/2])
		}
	}
	var lmao [][]XRangeSerialized
	added := false
	for i := 0; i < len(streams); i++ {
		var data []XRangeSerialized
		stream := streams[i]
		id := ids[i]
		entries := GlobalStore.XRead(stream, id)
		for _, entry := range entries {
			element := XRangeSerialized{}
			element.id = entry.ID
			for key, value := range entry.Fields {
				element.fields = append(element.fields, key)
				element.fields = append(element.fields, value)
				added = true
			}
			data = append(data, element)
		}
		lmao = append(lmao, data)
	}
	if added {
		ans.stream = streams
		ans.entries = lmao
		return respAny(conn, ans)
	}
	if n != 0 {
		lmao = [][]XRangeSerialized{}
		if n > 0 {
			time.Sleep(time.Duration(n * int(time.Millisecond)))
		}
		for i := 0; i < len(streams); i++ {
			var data []XRangeSerialized
			stream := streams[i]
			id := ids[i]
			entries := GlobalStore.XRead(stream, id)
			for _, entry := range entries {
				element := XRangeSerialized{}
				element.id = entry.ID
				for key, value := range entry.Fields {
					element.fields = append(element.fields, key)
					element.fields = append(element.fields, value)
					added = true
				}
				data = append(data, element)
			}
			lmao = append(lmao, data)
		}
		if !added {
			if _, err := conn.Write([]byte("*-1\r\n")); err != nil {
				return err
			}
			return nil
		}
		ans.stream = streams
		ans.entries = lmao
		return respAny(conn, ans)
	} else {
		for {
			lmao = [][]XRangeSerialized{}
			time.Sleep(time.Duration(10 * int(time.Millisecond)))
			for i := 0; i < len(streams); i++ {
				var data []XRangeSerialized
				stream := streams[i]
				id := ids[i]
				entries := GlobalStore.XRead(stream, id)
				for _, entry := range entries {
					element := XRangeSerialized{}
					element.id = entry.ID
					for key, value := range entry.Fields {
						element.fields = append(element.fields, key)
						element.fields = append(element.fields, value)
						added = true
					}
					data = append(data, element)
				}
				lmao = append(lmao, data)
			}
			if added {
				break
			}
		}
		ans.stream = streams
		ans.entries = lmao
		return respAny(conn, ans)
	}
}

func handleXrange(conn net.Conn, stream, start, end string) error {
	entries := GlobalStore.XRange(stream, start, end)
	var data []XRangeSerialized
	for _, entry := range entries {
		element := XRangeSerialized{}
		element.id = entry.ID
		for key, value := range entry.Fields {
			element.fields = append(element.fields, key)
			element.fields = append(element.fields, value)
		}
		data = append(data, element)
	}
	return respAny(conn, data)
}

func handleXadd(conn net.Conn, stream string, id string, args []string) error {
	if err := verifyId(stream, id); err != nil {
		return respWriter(conn, ERROR, err.Error())
	}
	id, err := completeId(stream, id)
	if err != nil {
		return err
	}
	tmp := make(map[string]string)
	for i := 0; i < len(args); i += 2 {
		tmp[args[i]] = args[i+1]
	}
	GlobalStore.XAdd(stream, id, tmp)
	return respWriter(conn, BULK, id)
}

func handleType(conn net.Conn, key string) error {
	_, ok := GlobalStore.Get(key)
	if ok {
		return respWriter(conn, SIMPLE, "string")
	}
	_, ok = GlobalStore.streams[key]
	if ok {
		return respWriter(conn, SIMPLE, "stream")
	}
	return respWriter(conn, BULK, "none")
}

func handleBlpop(conn net.Conn, key, wait string) error {
	waitTime, err := strconv.ParseFloat(wait, 64)
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
	ch := make(chan string, 1)
	defer close(ch)
	GlobalStore.blockedChannels[key] = append(GlobalStore.blockedChannels[key], ch)
	time.Sleep(time.Duration(waitTime * float64(time.Second)))
	select {
	case val := <-ch:
		return respArray(conn, []string{key, val})
	default:
		mutex := GlobalStore.GetListMutex(key)
		mutex.Lock()
		defer mutex.Unlock()
		chans := GlobalStore.blockedChannels[key]
		for i, c := range chans {
			if c == ch {
				GlobalStore.blockedChannels[key] = append(chans[:i], chans[i+1:]...)
				break
			}
		}
		_, err := conn.Write([]byte("*-1\r\n"))
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
