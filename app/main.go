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
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func handleEcho(conn net.Conn, str string) error {
	msg := fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
	if _, err := conn.Write([]byte(msg)); err != nil {
		return err
	}
	return nil
}

func handlePing(conn net.Conn) error {
	if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
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
