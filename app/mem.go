package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
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
	EventCheckBlTimeout
)

type Store struct {
	store map[string]item
	t     *time.Ticker
	ch    chan Event
}

func NewStore(ch chan Event) *Store {
	return &Store{
		store: map[string]item{},
		t:     time.NewTicker(1 * time.Second),
		ch:    ch,
	}
}

type blStatus struct {
	result  chan []byte
	start   int64
	timeout int64
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

type pair struct {
	key   string
	value string
}

type entry struct {
	id    entryID
	pairs []pair
}

func (e *entry) String() string {
	return string(e.id)
}

func (e entry) ToArray() RESP {
	id := BulkString{string(e.id)}
	ps := Array{}
	for _, p := range e.pairs {
		ps.elements = append(ps.elements, BulkString{string(p.key)}, BulkString{string(p.value)})
	}
	return Array{
		elements: []RESP{id, ps},
	}
}

type entryID string

type eidGenType int

const (
	manualEID      eidGenType = 0
	partialAutoEID eidGenType = 1
	fullAutoEID    eidGenType = 2
	errEID         eidGenType = 255
)

func (a entryID) autoGen(lastID entryID) (eidGenType, entryID) {
	_, lastts, lastsid := lastID.Validate()
	ida := strings.SplitN(string(a), "-", 2)
	sid := 0
	if len(ida) < 2 {
		if ida[0] == "*" {
			now := time.Now().UnixMilli()
			if now == lastts {
				sid = int(lastsid) + 1
			}
			return fullAutoEID, entryID(strconv.Itoa(int(now)) + "-" + strconv.Itoa(sid))
		}
		return errEID, ""
	}
	if ida[1] == "*" {
		mants, _ := strconv.ParseInt(ida[0], 10, 64)
		if mants == lastts {
			sid = int(lastsid) + 1
		}
		return partialAutoEID, entryID(ida[0] + "-" + strconv.Itoa(sid))
	}

	return manualEID, a
}

func (a entryID) Validate() (bool, int64, int64) {
	ida := strings.SplitN(string(a), "-", 2)
	if len(ida) < 2 {
		if ida[0] != "*" {
			ts, err := strconv.ParseInt(ida[0], 10, 64)
			if err != nil {
				return false, -1, -1
			}
			return true, ts, -1
		}
		// for id = "*"
		return true, -1, -1
	}
	ts, err := strconv.ParseInt(ida[0], 10, 64)
	if err != nil {
		return false, -1, -1
	}
	id, err := strconv.ParseInt(ida[1], 10, 64)
	if err != nil && ida[1] != "*" {
		sid, err := strconv.ParseInt(ida[1], 10, 64)
		if err != nil {
			return false, -1, -1
		}
		return false, ts, sid
	}
	if err != nil {
		// for id = "ts"-"*"
		id = -1
	}
	return true, ts, id
}

// Greaterreturns whether a >= b
func (a entryID) Greater(b entryID) bool {
	v1, ts1, id1 := a.Validate()
	v2, ts2, id2 := b.Validate()
	if !v1 || !v2 {
		return false
	}
	if ts1 == ts2 {
		// NOTE: we assure no dups
		return id1 > id2
	}
	return ts1 > ts2
}

// GreaterOrEqual returns whether a >= b
func (a entryID) GreaterOrEqual(b entryID) bool {
	v1, ts1, id1 := a.Validate()
	v2, ts2, id2 := b.Validate()
	if !v1 || !v2 {
		return false
	}
	if ts1 == ts2 {
		// NOTE: we assure no dups
		return id1 >= id2
	}
	return ts1 >= ts2
}

type Stream struct {
	key     string
	entries map[string]*entry
	lastId  entryID
}

type sortedEntries []entry

type BlockableList struct {
	key             string
	list            deque.Deque[any]
	blockingClients []*clientStatus
	close           chan int
	eventCh         chan Event
}

func newBlockableList(key string, eventCh chan Event) *BlockableList {
	bl := &BlockableList{
		list:            deque.Deque[any]{},
		key:             key,
		blockingClients: []*clientStatus{},
		close:           make(chan int),
		eventCh:         eventCh,
	}
	go func() {
		t := time.NewTicker(50 * time.Millisecond)
	loop:
		for {
			select {
			case <-t.C:
				eventCh <- Event{
					Type: EventCheckBlTimeout,
					Data: key,
				}
			case <-bl.close:
				log.Printf("blocklist closed")
				break loop
			}
		}
	}()
	return bl
}

// getRawValue returns the internal type and represention of data: string, slice, or other structs
// not `item`, neither `BulkString` (binary/RESP)
func (s *Store) getRawValue(key string) (any, string) {
	if val, ok := s.store[key]; !ok || val.ts > 0 && val.ts < time.Now().UnixMilli() {
		return nil, "none"
	} else {
		switch v := val.data.(type) {
		case string:
			return v, "string"
		case *BlockableList:
			return v, "list"
		case *Stream:
			return v, "stream"
		default:
			panic(fmt.Sprintf("unsupported internal type: %T", val.data))
		}
	}
}

func (s *Store) nonBlockingLPOP(key string) (RESP, bool) {
	val, _ := s.getRawValue(key)
	if val == nil {
		return nil, false
	}
	cur := s.store[key].data.(*BlockableList)
	if cur.list.Len() == 0 {
		return nil, false
	}
	return cur.list.PopFront().(RESP), true
}

func (s *Store) start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Printf("loop cancelled")
			return nil
		case <-s.t.C:
			// log.Printf("store timely work")
		case ev, ok := <-s.ch:
			if !ok {
				log.Printf("event channel closed")
				return nil
			}
			s.handleEvent(ev)
			// log.Printf("event got: %#v", ev)
		}
	}
}

func (s *Store) handleEvent(ev Event) error {
	switch ev.Type {
	case EventCheckBlTimeout:
		key := ev.Data.(string)
		cur := s.store[key].data.(*BlockableList)
		next := []*clientStatus{}
		for i, c := range cur.blockingClients {
			s := c.status.(blStatus)
			if time.Now().UnixMilli()-s.start >= s.timeout {
				log.Printf("client removed: %d", i)
				s.result <- nullArray
			} else {
				next = append(next, c)
			}
		}
		cur.blockingClients = next

	case EventCmd:
		msg := ev.Data.(Array)
		if cmd, ok := msg.elements[0].(BulkString); ok {
			command := strings.ToUpper(cmd.content)
			switch command {
			case "XREAD":
				_ = msg.elements[1].(BulkString).content // optional
				streamKey := msg.elements[2].(BulkString).content
				id := msg.elements[3].(BulkString).content
				_, ts1, seq1 := entryID(id).Validate()
				if ts1 == -1 {
					if id != "-" {
						settleClient(ev.client, streamKey,
							SimpleError{"Invalid <start> for XRANGE"}.Encode())
					} else {
						ts1 = 0
					}
				}
				if seq1 == -1 {
					id = strconv.Itoa(int(ts1)) + "-" + "0"
				}

				val, t := s.getRawValue(streamKey)
				if val == nil {
					settleClient(ev.client, streamKey, nullBulkString)
					return nil
				} else {
					if t != "stream" {
						panic(fmt.Sprintf("%v is %s, not 'stream'", streamKey, t))
					}
				}
				// WARNING: it is very inefficient to call the sort process everytime!! But the standard Redis implementation is O(N) for N elements, you can optimize if you care about it
				stream := s.store[streamKey].data.(*Stream)
				sortedEntries := make([]*entry, 0, len(stream.entries))
				for _, v := range stream.entries {
					sortedEntries = append(sortedEntries, v)
				}
				log.Printf("sortedEntries: %v", sortedEntries)
				log.Printf("map sortedEntries: %v", stream.entries)
				sort.Slice(sortedEntries, func(i, j int) bool {
					return !sortedEntries[i].id.GreaterOrEqual(sortedEntries[j].id)
				})
				i := sort.Search(len(sortedEntries), func(k int) bool {
					return sortedEntries[k].id.Greater(entryID(id))
				})
				log.Printf("<id>: %v, i: %v", id, i)
				res := sortedEntries[i:]
				log.Printf("res: %v", res)
				elements := make([]RESP, len(res))
				for k, ent := range res {
					elements[k] = ent.ToArray()
				}
				result := Array{elements: []RESP{}}
				result.elements = append(result.elements, Array{
					elements: []RESP{BulkString{streamKey}, Array{elements: elements}},
				})
				settleClient(ev.client, streamKey, result.Encode())
				return nil

			case "XRANGE":
				streamKey := msg.elements[1].(BulkString).content
				start := msg.elements[2].(BulkString).content
				end := msg.elements[3].(BulkString).content
				_, ts1, seq1 := entryID(start).Validate()
				_, ts2, seq2 := entryID(end).Validate()
				if ts1 == -1 {
					if start != "-" {
						settleClient(ev.client, streamKey,
							SimpleError{"Invalid <start> for XRANGE"}.Encode())
					} else {
						ts1 = 0
					}
				}
				if seq1 == -1 {
					start = strconv.Itoa(int(ts1)) + "-" + "0"
				}

				if ts2 == -1 {
					if end != "+" {
						settleClient(ev.client, streamKey,
							SimpleError{"Invalid <end> for XRANGE"}.Encode())
					} else {
						ts2 = math.MaxInt64
					}
				}

				if seq2 == -1 {
					end = strconv.Itoa(int(ts2)) + "-" + strconv.Itoa(math.MaxInt64)
				} else {
					end = strconv.Itoa(int(ts2)) + "-" + strconv.Itoa(int(seq2+1))
				}
				val, t := s.getRawValue(streamKey)
				if val == nil {
					settleClient(ev.client, streamKey, nullBulkString)
					return nil
				} else {
					if t != "stream" {
						panic(fmt.Sprintf("%v is %s, not 'stream'", streamKey, t))
					}
				}
				// WARNING: it is very inefficient to call the sort process everytime!! But the standard Redis implementation is O(N) for N elements, you can optimize if you care about it
				stream := s.store[streamKey].data.(*Stream)
				sortedEntries := make([]*entry, 0, len(stream.entries))
				for _, v := range stream.entries {
					sortedEntries = append(sortedEntries, v)
				}
				log.Printf("sortedEntries: %v", sortedEntries)
				log.Printf("map sortedEntries: %v", stream.entries)
				sort.Slice(sortedEntries, func(i, j int) bool {
					return !sortedEntries[i].id.GreaterOrEqual(sortedEntries[j].id)
				})
				i := sort.Search(len(sortedEntries), func(k int) bool {
					return sortedEntries[k].id.GreaterOrEqual(entryID(start))
				})
				// jth is the first element that strictly Greater than the original end
				j := sort.Search(len(sortedEntries), func(k int) bool {
					return sortedEntries[k].id.GreaterOrEqual(entryID(end))
				})
				log.Printf("start: %v, end: %v, i: %v, j: %v", start, end, i, j)
				// [i, j-1]
				res := sortedEntries[i:j]
				log.Printf("res: %v", res)
				elements := make([]RESP, len(res))
				for k, ent := range res {
					elements[k] = ent.ToArray()
				}
				settleClient(ev.client, streamKey, Array{
					elements: elements,
				}.Encode())
				return nil

			case "XADD":
				// NOTE: we only support explicit id for now.
				streamKey := msg.elements[1].(BulkString).content
				id := msg.elements[2].(BulkString).content
				val, t := s.getRawValue(streamKey)
				if val == nil {
					s.store[streamKey] = item{
						data: &Stream{key: streamKey,
							entries: make(map[string]*entry),
							lastId:  "0-0",
						},
						ts: -1,
					}
				} else {
					if t != "stream" {
						panic(fmt.Sprintf("%v is %s, not 'stream'", id, t))
					}
				}
				stream := s.store[streamKey].data.(*Stream)
				_, eid := entryID(id).autoGen(stream.lastId)
				log.Printf("auto genid for %v => %v", id, eid)
				ok, ts, seqID := eid.Validate()
				// TODO: negative?
				if !ok || (ts == 0 && seqID == 0) {
					settleClient(ev.client, streamKey,
						SimpleError{"ERR The ID specified in XADD must be greater than 0-0"}.Encode())
					return nil
				}
				if !eid.Greater(stream.lastId) {
					settleClient(ev.client, "",
						SimpleError{"ERR The ID specified in XADD is equal or smaller than the target stream top item"}.Encode(),
					)
					return nil
				}

				stream.lastId = eid
				stream.entries[string(eid)] = &entry{
					id:    eid,
					pairs: []pair{},
				}
				for i := 3; i+1 < len(msg.elements); i += 2 {
					key := msg.elements[i].(BulkString).content
					value := msg.elements[i+1].(BulkString).content
					stream.entries[string(eid)].pairs = append(stream.entries[string(eid)].pairs, pair{key, value})
				}
				settleClient(ev.client, "", BulkString{string(eid)}.Encode())
			case "TYPE":
				key := msg.elements[1].(BulkString).content
				_, t := s.getRawValue(key)
				settleClient(ev.client, "", []byte("+"+t+"\r\n"))
			case "PING":
				settleClient(ev.client, "", []byte("+PONG\r\n"))
			case "ECHO":
				key := msg.elements[1].(BulkString)
				settleClient(ev.client, key.content, key.Encode())
			case "GET":
				key := msg.elements[1].(BulkString).content
				if val, _ := s.getRawValue(key); val == nil {
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
				val, _ := s.getRawValue(listKey)
				if val == nil {
					s.store[listKey] = item{
						data: newBlockableList(listKey, s.ch),
						ts:   -1,
					}
				}
				cur := s.store[listKey].data.(*BlockableList)
				if cur.list.Len() == 0 {
					var timeout float64 = 24 * 365 * 10 * 3600
					if len(msg.elements) >= 3 {
						timeout = toFloat(msg.elements[2]) * 1000
						log.Printf("block duration: %v", timeout)
					}
					bl := blStatus{
						result:  make(chan []byte),
						start:   time.Now().UnixMilli(),
						timeout: int64(timeout),
					}
					cur.blockingClients = append(cur.blockingClients, &clientStatus{
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
				val, _ := s.getRawValue(listKey)
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
				val, _ := s.getRawValue(listKey)
				if val == nil {
					s.store[listKey] = item{
						data: newBlockableList(listKey, s.ch),
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

				// wake up blocking clients if possible
				next := []*clientStatus{}
				for _, c := range cur.blockingClients {
					v, got := s.nonBlockingLPOP(c.blockingKey)
					if got {
						res := Array{elements: []RESP{
							BulkString{c.blockingKey}, v,
						}}
						log.Printf("trying to send result: %#v", res)
						c.status.(blStatus).result <- res.Encode()
						log.Printf("[over]trying to send result: %#v", res)
					} else {
						next = append(next, c)
					}
				}
				cur.blockingClients = next
				settleClient(ev.client, listKey, Integer{content: length}.Encode())
			case "LRANGE":
				listKey := msg.elements[1].(BulkString).content
				val, _ := s.getRawValue(listKey)
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
				val, _ := s.getRawValue(listKey)
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
