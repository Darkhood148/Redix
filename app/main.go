package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
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
	TYPE   = "TYPE"
	XADD   = "XADD"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var GlobalStore = NewStore()

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
		case TYPE:
			if err = handleType(conn, args[1]); err != nil {
				return err
			}
		case XADD:
			if err = handleXadd(conn, args[1], args[2], args[3:]); err != nil {
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
