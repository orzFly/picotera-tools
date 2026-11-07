package store

import (
	"container/list"
	"sync"
	"time"
)

// lru evicts by total cost, not count, so a few huge entries cannot pin the budget. Items idle
// longer than idle are dropped too: archived days are read in short bursts from the dashboard,
// and the hundreds of MiB a burst caches should not sit there until the next one.
type lru[K comparable, V any] struct {
	mu    sync.Mutex
	max   int64
	cur   int64
	idle  time.Duration
	now   func() time.Time
	ll    *list.List
	items map[K]*list.Element
	cost  func(V) int64
}

type lruItem[K comparable, V any] struct {
	key  K
	val  V
	used time.Time
}

func newLRU[K comparable, V any](max int64, idle time.Duration, cost func(V) int64) *lru[K, V] {
	return &lru[K, V]{max: max, idle: idle, now: time.Now, ll: list.New(), items: map[K]*list.Element{}, cost: cost}
}

func (c *lru[K, V]) get(k K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero V
	el, ok := c.items[k]
	if !ok {
		return zero, false
	}
	it := el.Value.(*lruItem[K, V])
	now := c.now()
	if c.idle > 0 && now.Sub(it.used) > c.idle {
		c.remove(el)
		return zero, false
	}
	it.used = now
	c.ll.MoveToFront(el)
	return it.val, true
}

func (c *lru[K, V]) add(k K, v V) {
	n := c.cost(v)
	if n > c.max {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if el, ok := c.items[k]; ok {
		it := el.Value.(*lruItem[K, V])
		c.cur += n - c.cost(it.val)
		it.val, it.used = v, now
		c.ll.MoveToFront(el)
	} else {
		c.items[k] = c.ll.PushFront(&lruItem[K, V]{k, v, now})
		c.cur += n
	}
	for c.cur > c.max {
		c.remove(c.ll.Back())
	}
}

// expire drops everything idle too long. Recency order is also idleness order, so it stops at
// the first item still in use.
func (c *lru[K, V]) expire() int {
	if c.idle <= 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	n := 0
	for el := c.ll.Back(); el != nil; el = c.ll.Back() {
		if now.Sub(el.Value.(*lruItem[K, V]).used) <= c.idle {
			break
		}
		c.remove(el)
		n++
	}
	return n
}

func (c *lru[K, V]) remove(el *list.Element) {
	it := el.Value.(*lruItem[K, V])
	c.ll.Remove(el)
	delete(c.items, it.key)
	c.cur -= c.cost(it.val)
}

func (c *lru[K, V]) size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur
}
