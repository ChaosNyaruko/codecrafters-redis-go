package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type Array struct {
	elements []any
}

type BulkString struct {
	content string
}

type Decoder struct {
	s *bufio.Scanner
}

func (d *Decoder) Decode(data []byte) (any, error) {
	header, content, found := bytes.Cut(data, []byte{'\r', '\n'})
	if !found {
		panic("unreachable")
	}

	t := header[0]
	switch t {
	case '$': // bulk string
		length, err := strconv.ParseInt(string(header[1:]), 10, 64)
		if err != nil {
			panic(err)
		}
		return BulkString{string(content[:length])}, nil
	case '*':
		count, err := strconv.ParseInt(string(header[1:]), 10, 64)
		if err != nil {
			panic(err)
		}
		arr := Array{
			elements: make([]any, int(count)),
		}
		for i := 0; i < int(count); i++ {
			if d.s.Scan() {
				arr.elements[i], err = d.Decode(d.s.Bytes())
				if err != nil {
					panic(err)
				}
			} else {
				panic(d.s.Err())
			}
		}
		return arr, nil
	default:
		panic(fmt.Sprintf("not supported, %v", t))
	}
}

func writeWithBail(conn net.Conn, data []byte) {
	_, err := conn.Write(data)
	if err != nil {
		log.Print("Error Write Conn: ", err.Error())
		os.Exit(1)
	}
}

func (bs BulkString) Encode() []byte {
	var res = make([]byte, 0, len(bs.content)+1+10)
	res = append(res, '$')
	length := strconv.Itoa(len(bs.content))
	res = append(res, []byte(length)...)
	res = append(res, "\r\n"...)
	res = append(res, []byte(bs.content)...)
	res = append(res, "\r\n"...)
	return res
}

func split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	header, content, found := bytes.Cut(data, []byte{'\r', '\n'})
	if !found {
		return 0, nil, nil
	}
	t := header[0]
	switch t {
	case '$': // bulk string
		length, err := strconv.ParseInt(string(header[1:]), 10, 64)
		if err != nil {
			return 0, nil, err
		}
		if int64(len(content)) < length {
			return 0, nil, nil
		}
		totalLength := len(header) + 4 + int(length)
		return totalLength, data[:totalLength], nil
	case '*': // array
		_, err := strconv.ParseInt(string(header[1:]), 10, 64)
		if err != nil {
			return 0, nil, err
		}
		totalLength := len(header) + 2
		return totalLength, data[:totalLength], nil
	default:
		panic(fmt.Sprintf("not supported, %v", t))
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
			continue
		}
		go func() {
			scanner := bufio.NewScanner(conn)
			scanner.Split(split)
			decoder := Decoder{
				scanner,
			}
			for scanner.Scan() {
				data := scanner.Bytes()
				log.Printf("%v, data: %v", conn.RemoteAddr(), data)
				msg, err := decoder.Decode(data)
				if err != nil {
					log.Printf("decode data err: %v", err.Error())
					os.Exit(1)
				}
				switch msg := msg.(type) {
				case BulkString:
					log.Printf("msg: bulk  string: %v", msg)
				case Array:
					log.Printf("msg: array %v", msg)
					if cmd, ok := msg.elements[0].(BulkString); ok {
						switch strings.ToUpper(cmd.content) {
						case "PING":
							writeWithBail(conn, []byte("+PONG\r\n"))
						case "ECHO":
							key := msg.elements[1].(BulkString)
							writeWithBail(conn, key.Encode())
						default:
							panic(fmt.Sprintf("unsupported command: %v", cmd.content))
						}
					} else {
						panic("command should be a bulk string")
					}
				default:
					panic("unknown")
				}
			}

			if scanner.Err() != nil {
				log.Printf("scanner err: %v", scanner.Err())
				os.Exit(69)
			}
		}()
	}
}
