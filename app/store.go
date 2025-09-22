package main

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

type StoreValue struct {
	value     string
	expiresAt time.Time
}

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

type Store struct {
	data            map[string]StoreValue
	lists           map[string][]string
	mutList         map[string]*sync.RWMutex
	blockedChannels map[string][]chan string
	streams         map[string][]StreamEntry
}

func NewStore() *Store {
	return &Store{
		data:            make(map[string]StoreValue),
		lists:           make(map[string][]string),
		mutList:         make(map[string]*sync.RWMutex),
		blockedChannels: make(map[string][]chan string),
		streams:         make(map[string][]StreamEntry),
	}
}

func (s *Store) Set(key string, value StoreValue) {
	s.data[key] = StoreValue{value: value.value, expiresAt: value.expiresAt}
}

func (s *Store) Get(key string) (StoreValue, bool) {
	val, ok := s.data[key]
	return val, ok
}

func (s *Store) GetMutex(key string) *sync.RWMutex {
	if _, ok := s.mutList[key]; !ok {
		s.mutList[key] = &sync.RWMutex{}
	}
	return s.mutList[key]
}

func (s *Store) Rpush(key string, value []string) {
	val, ok := s.lists[key]
	mutex := s.GetMutex(key)
	mutex.Lock()
	defer mutex.Unlock()
	if ok {
		s.lists[key] = append(val, value...)
	} else {
		s.lists[key] = value
	}
}

func (s *Store) Lpush(key string, value []string) {
	val, ok := s.lists[key]
	mutex := s.GetMutex(key)
	mutex.Lock()
	defer mutex.Unlock()
	if ok {
		slices.Reverse(value)
		s.lists[key] = append(value, val...)
	} else {
		s.lists[key] = value
	}
}

func (s *Store) LRange(key string, start, stop int) []string {
	mutex := s.GetMutex(key)
	mutex.RLock()
	defer mutex.RUnlock()
	if start >= len(s.lists[key]) {
		return []string{}
	}
	return s.lists[key][start:min(stop+1, len(s.lists[key]))]
}

func (s *Store) LPop(key string) string {
	mutex := s.GetMutex(key)
	mutex.Lock()
	defer mutex.Unlock()
	val, ok := s.lists[key]
	if !ok {
		return ""
	} else {
		value := val[0]
		s.lists[key] = val[1:]
		return value
	}
}

func (s *Store) LPopMultiple(key string, num int) []string {
	mutex := s.GetMutex(key)
	mutex.Lock()
	defer mutex.Unlock()
	val, ok := s.lists[key]
	if !ok {
		return []string{}
	} else {
		values := val[:num]
		s.lists[key] = val[num:]
		return values
	}
}

func (s *Store) XAdd(stream, id string, fields map[string]string) string {
	if id == "*" {
		id = fmt.Sprintf("%d-0", time.Now().UnixNano()/1e6)
	}
	entry := StreamEntry{ID: id, Fields: fields}
	s.streams[stream] = append(s.streams[stream], entry)
	return id
}
