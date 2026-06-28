package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gammazero/deque"
)

type item struct {
	data any
	ts   int64 // expired unix timestamp, milliseconds
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

// getRawValue returns the internal represention of data: string, slice, or other structs
// not `item`, neither `BulkString` (binary/RESP)s
func (s *Store) getRawValue(key string) any {
	if val, ok := s.store[key]; !ok || val.ts > 0 && val.ts < time.Now().UnixMilli() {
		return nil
	} else {
		switch v := val.data.(type) {
		case string:
			return v
		case deque.Deque[any]:
			return v
		default:
			panic(fmt.Sprintf("unknown internal type: %T", val.data))
		}
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
	switch ev.Type {
	case EventCmd:
		msg := ev.Data.(Array)
		if cmd, ok := msg.elements[0].(BulkString); ok {
			command := strings.ToUpper(cmd.content)
			switch command {
			case "PING":
				writeWithBail(ev.conn, []byte("+PONG\r\n"))
			case "ECHO":
				key := msg.elements[1].(BulkString)
				writeWithBail(ev.conn, key.Encode())
			case "GET":
				key := msg.elements[1].(BulkString).content
				if val := s.getRawValue(key); val == nil {
					writeWithBail(ev.conn, nullBulkString)
				} else {
					bv := BulkString{val.(string)}
					writeWithBail(ev.conn, bv.Encode())
				}
			case "SET":
				key := msg.elements[1].(BulkString).content
				value := msg.elements[2].(BulkString).content
				var expired int64 = -1
				if len(msg.elements) >= 4 {
					if ex, ok := msg.elements[3].(BulkString); ok {
						ex := strings.ToUpper(ex.content)
						t := toInt(msg.elements[4])
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
					data: value,
					ts:   expired,
				}
				writeWithBail(ev.conn, OK)
			case "RPUSH", "LPUSH":
				listKey := msg.elements[1].(BulkString).content
				val := s.getRawValue(listKey)
				if val == nil {
					s.store[listKey] = item{
						data: deque.Deque[any]{},
						ts:   -1,
					}
				}
				cur := s.store[listKey].data.(deque.Deque[any])
				values := msg.elements[2:]
				for _, v := range values {
					if command == "RPUSH" {
						cur.PushBack(v)
					} else {
						cur.PushFront(v)
					}
				}
				length := int64(cur.Len())

				s.store[listKey] = item{
					data: cur,
					ts:   -1,
				}
				writeWithBail(ev.conn, Integer{content: length}.Encode())
			case "LRANGE":
				listKey := msg.elements[1].(BulkString).content
				val := s.getRawValue(listKey)
				if val == nil {
					writeWithBail(ev.conn, Array{}.Encode())
					return nil
				}
				cur := s.store[listKey].data.(deque.Deque[any])
				start := toInt(msg.elements[2])
				if start < 0 {
					start = max(start+cur.Len(), 0)
				}
				stop := toInt(msg.elements[3])
				if stop < 0 {
					stop = max(stop+cur.Len(), 0)
				}
				stop = min(cur.Len()-1, stop)
				log.Printf("LRANGE: [%d, %d]", start, stop)
				res := Array{
					elements: make([]RESP, stop-start+1),
				}
				for i := start; i <= stop; i++ {
					res.elements[i-start] = cur.At(i).(RESP)
				}
				writeWithBail(ev.conn, res.Encode())

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

func toInt(v RESP) int {
	switch raw := v.(type) {
	case BulkString:
		val, err := strconv.Atoi(raw.content)
		if err != nil {
			panic(err)
		}
		return val
	case Integer:
		return int(raw.content)
	default:
		panic("cannot parse expiry time as int")
	}
}
