package main

import (
	"bytes"
	"fmt"
	"net"
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
