package lru_test

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Maksim-Burtsev/go-reading/apps/05-lru-cache/lru"
)

func benchKeys(n int) []string {
	keys := make([]string, n)
	for i := range n {
		keys[i] = "key-" + strconv.Itoa(i)
	}
	return keys
}

func BenchmarkGetHit(b *testing.B) {
	keys := benchKeys(1024)
	c := newCache(b, len(keys))
	for i, k := range keys {
		c.Set(k, i)
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		c.Get(keys[i%len(keys)])
		i++
	}
}

func BenchmarkSet(b *testing.B) {
	keys := benchKeys(4096)
	c := newCache(b, 1024)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		c.Set(keys[i%len(keys)], i)
		i++
	}
}

func BenchmarkParallelMixed(b *testing.B) {
	keys := benchKeys(2048)
	c := newCache(b, 1024, lru.WithTTL[string, int](time.Minute))
	for i, k := range keys {
		c.Set(k, i)
	}
	var worker atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := int(worker.Add(1)) * 7919
		for pb.Next() {
			key := keys[i%len(keys)]
			if i%4 == 0 {
				c.Set(key, i)
			} else {
				c.Get(key)
			}
			i++
		}
	})
}
