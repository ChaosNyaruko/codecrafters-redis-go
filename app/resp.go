package main

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
)

// constants
var nullBulkString = []byte("$-1\r\n")
var OK = []byte("+OK\r\n")

type Array struct {
	elements []any
}

type Integer struct {
	content int64
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
	case ':': // integers
		value, err := strconv.ParseInt(string(header[1:]), 10, 64)
		if err != nil {
			panic(err)
		}
		return Integer{value}, nil
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
	case ':': // integers
		totalLength := len(header) + 2
		return totalLength, data[:totalLength], nil
	default:
		panic(fmt.Sprintf("not supported, %v", t))
	}
}
