package main

import (
	"context"
	"fmt"
	"log"
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

type Store struct {
	store map[string]item
	t     *time.Ticker

	blockingClients map[string][]*clientStatus
}

func NewStore() *Store {
	return &Store{
		store:           map[string]item{},
		t:               time.NewTicker(50 * time.Nanosecond),
		blockingClients: map[string][]*clientStatus{},
	}
}

type blStatus struct {
	result  chan []byte
	timeout *time.Timer
}

type clientStatus struct {
	blockingKey string
	status      any
}

type Event struct {
	Type   EventType
	Data   any
	client chan clientStatus
}

type BlockableList struct {
	list deque.Deque[any]
}

func newBlockableList() *BlockableList {
	return &BlockableList{
		list: deque.Deque[any]{},
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
		case *BlockableList:
			return v
		default:
			panic(fmt.Sprintf("unknown internal type: %T", val.data))
		}
	}
}

func (s *Store) nonBlockingLPOP(key string) (RESP, bool) {
	val := s.getRawValue(key)
	if val == nil {
		return nil, false
	}
	cur := s.store[key].data.(*BlockableList)
	if cur.list.Len() == 0 {
		return nil, false
	}
	return cur.list.PopFront().(RESP), true
}

func (s *Store) start(ctx context.Context, ch <-chan Event) error {
	for {
		select {
		case <-ctx.Done():
			log.Printf("loop cancelled")
			return nil
		case <-s.t.C:
			log.Printf("store timely work")
			for k, cs := range s.blockingClients {
				next := []*clientStatus{}
				log.Printf("processing blocking key: %v", k)
				for i, c := range cs {
					select {
					case <-c.status.(blStatus).timeout.C:
						log.Printf("blocking client timeout")
						c.status.(blStatus).result <- nullArray
					default:
						v, got := s.nonBlockingLPOP(c.blockingKey)
						if !got {
							next = append(next, cs[i:]...)
							s.blockingClients[k] = next
							break
						}
						res := Array{elements: []RESP{
							BulkString{k}, v,
						}}
						log.Printf("trying to send result: %#v", res)
						c.status.(blStatus).result <- res.Encode()
						log.Printf("[over]trying to send result: %#v", res)
					}
				}
				if len(next) == 0 {
					delete(s.blockingClients, k)
				}
			}
		case ev, ok := <-ch:
			if !ok {
				log.Printf("event channel closed")
				return nil
			}
			s.handleEvent(ev)
			log.Printf("event got: %#v", ev)
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
				settleClient(ev.client, "", []byte("+PONG\r\n"))
			case "ECHO":
				key := msg.elements[1].(BulkString)
				settleClient(ev.client, key.content, key.Encode())
			case "GET":
				key := msg.elements[1].(BulkString).content
				if val := s.getRawValue(key); val == nil {
					settleClient(ev.client, key, nullBulkString)
				} else {
					bv := BulkString{val.(string)}
					settleClient(ev.client, key, bv.Encode())
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
				settleClient(ev.client, key, OK)
			case "BLPOP":
				listKey := msg.elements[1].(BulkString).content
				val := s.getRawValue(listKey)
				if val == nil {
					s.store[listKey] = item{
						data: newBlockableList(),
						ts:   -1,
					}
				}
				cur := s.store[listKey].data.(*BlockableList)
				if cur.list.Len() == 0 {
					timeout := time.Hour * 24 * 365 * 10
					if len(msg.elements) >= 3 {
						timeout = time.Duration(toFloat(msg.elements[2]) * float64(time.Second))
						log.Printf("block duraiton: %v", timeout)
					}
					bl := blStatus{
						result:  make(chan []byte),
						timeout: time.NewTimer(timeout),
					}
					s.blockingClients[listKey] = append(s.blockingClients[listKey], &clientStatus{
						blockingKey: listKey,
						status:      bl,
					})
					settleClient(ev.client, listKey, bl)
				} else {
					res := cur.list.PopFront().(RESP).Encode()
					settleClient(ev.client, listKey, res)
				}
			case "RPOP", "LPOP":
				listKey := msg.elements[1].(BulkString).content
				val := s.getRawValue(listKey)
				if val == nil {
					settleClient(ev.client, listKey, nullBulkString)
					return nil
				}
				cur := s.store[listKey].data.(*BlockableList)
				if cur.list.Len() == 0 {
					settleClient(ev.client, listKey, nullBulkString)
					return nil
				}
				num := 1
				res := Array{elements: []RESP{}}
				array := false
				if len(msg.elements) >= 3 {
					num = toInt(msg.elements[2])
					log.Printf("POP num: %d", num)
					array = true
				}
				for num > 0 {
					if cur.list.Len() == 0 {
						break
					}
					if command == "RPOP" {
						res.elements = append(res.elements, cur.list.PopBack().(RESP))
					} else {
						res.elements = append(res.elements, cur.list.PopFront().(RESP))
					}
					num -= 1
				}
				if array {
					settleClient(ev.client, listKey, res.Encode())
				} else {
					settleClient(ev.client, listKey, res.elements[0].Encode())
				}

			case "RPUSH", "LPUSH":
				listKey := msg.elements[1].(BulkString).content
				val := s.getRawValue(listKey)
				if val == nil {
					s.store[listKey] = item{
						data: newBlockableList(),
						ts:   -1,
					}
				}
				cur := s.store[listKey].data.(*BlockableList)
				values := msg.elements[2:]
				for _, v := range values {
					if command == "RPUSH" {
						cur.list.PushBack(v)
					} else {
						cur.list.PushFront(v)
					}
				}
				length := int64(cur.list.Len())

				s.store[listKey] = item{
					data: cur,
					ts:   -1,
				}
				settleClient(ev.client, listKey, Integer{content: length}.Encode())
			case "LRANGE":
				listKey := msg.elements[1].(BulkString).content
				val := s.getRawValue(listKey)
				if val == nil {
					settleClient(ev.client, listKey, Array{}.Encode())
					return nil
				}
				cur := s.store[listKey].data.(*BlockableList)
				start := toInt(msg.elements[2])
				if start < 0 {
					start = max(start+cur.list.Len(), 0)
				}
				stop := toInt(msg.elements[3])
				if stop < 0 {
					stop = max(stop+cur.list.Len(), 0)
				}
				stop = min(cur.list.Len()-1, stop)
				log.Printf("LRANGE: [%d, %d]", start, stop)
				res := Array{
					elements: make([]RESP, stop-start+1),
				}
				for i := start; i <= stop; i++ {
					res.elements[i-start] = cur.list.At(i).(RESP)
				}
				settleClient(ev.client, listKey, res.Encode())
			case "LLEN":
				listKey := msg.elements[1].(BulkString).content
				val := s.getRawValue(listKey)
				res := Integer{0}
				if val != nil {
					cur := s.store[listKey].data.(*BlockableList)
					res.content = int64(cur.list.Len())
				}
				settleClient(ev.client, listKey, res.Encode())

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

func toFloat(v RESP) float64 {
	switch raw := v.(type) {
	case BulkString:
		val, err := strconv.ParseFloat(raw.content, 64) // s is string
		if err != nil {
			panic(err)
		}
		return val
	default:
		panic("cannot parse expiry time as float")
	}
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
