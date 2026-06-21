package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

type item struct {
	value any
	ts    int64 // expired unix timestamp, milliseconds
}

type EventType int

const (
	EventCmd = iota
)

type Event struct {
	Type EventType
	Data any
	conn net.Conn
}

type Store struct {
	store map[string]item
	t     *time.Ticker
}

func NewStore() *Store {
	return &Store{
		store: make(map[string]item),
		t:     time.NewTicker(1 * time.Second),
	}
}

func (s *Store) start(ctx context.Context, ch <-chan Event) error {
	for {
		select {
		case <-ctx.Done():
			log.Printf("loop cancelled")
			return nil
		case <-s.t.C:
			log.Printf("store timely work")
		case ev, ok := <-ch:
			if !ok {
				log.Printf("event channel closed")
				return nil
			}
			s.handleEvent(ev)
			log.Printf("event got: %v", ev)
		}
	}
}

func (s *Store) handleEvent(ev Event) error {
	var err error
	switch ev.Type {
	case EventCmd:
		msg := ev.Data.(Array)
		if cmd, ok := msg.elements[0].(BulkString); ok {
			switch strings.ToUpper(cmd.content) {
			case "PING":
				writeWithBail(ev.conn, []byte("+PONG\r\n"))
			case "ECHO":
				key := msg.elements[1].(BulkString)
				writeWithBail(ev.conn, key.Encode())
			case "GET":
				key := msg.elements[1].(BulkString).content
				if val, ok := s.store[key]; !ok || val.ts > 0 && val.ts < time.Now().UnixMilli() {
					writeWithBail(ev.conn, nullBulkString)
				} else {
					v := val.value.(string)
					bv := BulkString{v}
					writeWithBail(ev.conn, bv.Encode())
				}
			case "SET":
				key := msg.elements[1].(BulkString).content
				value := msg.elements[2].(BulkString).content
				var expired int64 = -1
				if len(msg.elements) >= 4 {
					if ex, ok := msg.elements[3].(BulkString); ok {
						ex := strings.ToUpper(ex.content)
						var t int
						switch raw := msg.elements[4].(type) {
						case BulkString:
							t, err = strconv.Atoi(raw.content)
							if err != nil {
								panic(err)
							}
						case Integer:
							t = int(raw.content)
						default:
							panic("cannot parse expiry time as int")
						}
						if ex == "EX" {
							expired = time.Now().Add(time.Duration(t) * time.Second).UnixMilli()
						} else if ex == "PX" {
							expired = time.Now().Add(time.Duration(t) * time.Millisecond).UnixMilli()
						} else {
							panic(fmt.Sprintf("unknown expiry: %v", ex))
						}
					}
				}
				s.store[key] = item{
					value: value,
					ts:    expired,
				}
				writeWithBail(ev.conn, OK)
			default:
				panic(fmt.Sprintf("unsupported command: %v", cmd.content))
			}
		} else {
			panic("command should be a bulk string")
		}
	default:
		return fmt.Errorf("unknown event: %v", ev)
	}
	return nil
}
