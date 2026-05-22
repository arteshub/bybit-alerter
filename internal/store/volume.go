package store

import (
	"sync"

	"volume_pump_checker/internal/exchange"
)

// VolumeStore is a thread-safe in-memory store for per-symbol 90-day average
// turnover and per-symbol last-seen intraday turnover (for crossing detection).
type VolumeStore struct {
	mu           sync.RWMutex
	avg          map[exchange.Symbol]float64
	lastTurnover map[exchange.Symbol]float64
}

func NewVolumeStore() *VolumeStore {
	return &VolumeStore{
		avg:          make(map[exchange.Symbol]float64),
		lastTurnover: make(map[exchange.Symbol]float64),
	}
}

func (s *VolumeStore) Set(sym exchange.Symbol, avg float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.avg[sym] = avg
}

func (s *VolumeStore) Get(sym exchange.Symbol) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.avg[sym]
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
	delete(s.avg, sym)
	delete(s.lastTurnover, sym)
}
