package store

import (
	"sync"

	"volume_pump_checker/internal/exchange"
)

// VolumeStore is a thread-safe in-memory store for per-symbol multi-period
// average turnover and per-symbol last-seen intraday turnover.
type VolumeStore struct {
	mu           sync.RWMutex
	avgs         map[exchange.Symbol]map[int]float64
	lastTurnover map[exchange.Symbol]float64
}

func NewVolumeStore() *VolumeStore {
	return &VolumeStore{
		avgs:         make(map[exchange.Symbol]map[int]float64),
		lastTurnover: make(map[exchange.Symbol]float64),
	}
}

func (s *VolumeStore) SetAvg(sym exchange.Symbol, days int, avg float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.avgs[sym] == nil {
		s.avgs[sym] = make(map[int]float64)
	}
	s.avgs[sym][days] = avg
}

func (s *VolumeStore) GetAvg(sym exchange.Symbol, days int) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.avgs[sym]
	if !ok {
		return 0, false
	}
	v, ok := m[days]
	return v, ok
}

// SwapTurnover atomically replaces the stored turnover for sym and returns the
// previous value. ok=false means this is the first update for this symbol.
func (s *VolumeStore) SwapTurnover(sym exchange.Symbol, current float64) (prev float64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, ok = s.lastTurnover[sym]
	s.lastTurnover[sym] = current
	return
}

func (s *VolumeStore) Delete(sym exchange.Symbol) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.avgs, sym)
	delete(s.lastTurnover, sym)
}
