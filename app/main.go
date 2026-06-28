package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func settleClient(client chan clientStatus, key string, status any) {
	cs := clientStatus{
		blockingKey: key,
		status:      status,
	}
	client <- cs
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan Event)
	store := NewStore()
	go store.start(ctx, events)
	for {
		conn, err := l.Accept()
		_ = conn.(*net.TCPConn)
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
			respCh := make(chan clientStatus)
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
					log.Printf("msg: array %#v", msg)
					events <- Event{
						Type:   EventCmd,
						Data:   msg,
						client: respCh,
					}
					resp := <-respCh
					status := resp.status
					switch s := status.(type) {
					case []byte:
						_, err := conn.Write(s)
						if err != nil {
							log.Print("Error Write Conn: ", err.Error())
							os.Exit(1)
						}
					case blStatus:
						data := <- s.result
						_, err := conn.Write(data)
						if err != nil {
							log.Print("Error Write Conn: ", err.Error())
							os.Exit(1)
						}
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
