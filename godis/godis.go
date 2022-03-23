package godis

import "sync"

type Godis[T any] struct {
	l   *sync.RWMutex
	m   map[string]any
	ttl map[string]int64
}

func NewGodis[T any]() (godis *Godis[T]) {
	godis = &Godis[T]{
		l:   &sync.RWMutex{},
		m:   map[string]any{},
		ttl: map[string]int64{},
	}
	return
}

func (g *Godis[T]) Set(k string, v T) {
	g.l.Lock()
	defer g.l.Unlock()
	g.m[k] = v
}
func (g *Godis[T]) Get(k string) (v T, ok bool) {
	g.l.RLock()
	defer g.l.RUnlock()
	v, ok = g.m[k].(T)
	return
}
