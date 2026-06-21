package main

import (
	"testing"
)

func Test_Encode(t *testing.T) {
	a := Array{}
	res := a.Encode()
	t.Logf("0-len array: %v", res)

	a = Array{
		elements: []RESP{BulkString{content: "hello"}},
	}
	res = a.Encode()
	t.Logf("0-len array: %v", res)

}
