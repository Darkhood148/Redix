package main

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"reflect"
)

type respStringType string

const (
	BULK    respStringType = "BULK"
	SIMPLE  respStringType = "SIMPLE"
	INTEGER respStringType = "INTEGER"
	ARRAY   respStringType = "ARRAY"
	ERROR   respStringType = "ERROR"
)

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

func respAny(conn net.Conn, data interface{}) error {
	switch t := data.(type) {
	case XRangeSerialized:
		entryMsg := "*2\r\n"
		if _, err := conn.Write([]byte(entryMsg)); err != nil {
			return err
		}
		if err := respWriter(conn, BULK, t.id); err != nil {
			return err
		}
		if err := respArray(conn, t.fields); err != nil {
			return err
		}
		return nil
	case []XRangeSerialized:
		msg := fmt.Sprintf("*%d\r\n", len(t))
		fmt.Println(len(t))
		fmt.Println("Wtf")
		if _, err := conn.Write([]byte(msg)); err != nil {
			return err
		}
		for _, elem := range t {
			if err := respAny(conn, elem); err != nil {
				return err
			}
		}
		return nil
	case XReadSerialized:
		msg := fmt.Sprintf("*%d\r\n", len(t.entries))
		if _, err := conn.Write([]byte(msg)); err != nil {
			return err
		}
		for i := 0; i < len(t.entries); i++ {
			startMsg := "*2\r\n"
			if _, err := conn.Write([]byte(startMsg)); err != nil {
				return err
			}
			if err := respWriter(conn, BULK, t.stream[i]); err != nil {
				return err
			}
			if err := respAny(conn, t.entries[i]); err != nil {
				return err
			}
		}
		return nil
	default:
		fmt.Println(reflect.TypeOf(t))
		return errors.New("unsupported data type")
	}
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
	case ERROR:
		msg = fmt.Sprintf("-%s\r\n", str)
	}
	if _, err := conn.Write([]byte(msg)); err != nil {
		return err
	}
	return nil
}
