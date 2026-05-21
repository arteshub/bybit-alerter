package store

import (
	"sync"

	"volume_pump_checker/internal/exchange"
)

// VolumeStore is a thread-safe in-memory store for per-symbol average turnover
// and per-symbol per-day alert deduplication.
type VolumeStore struct {
	mu           sync.RWMutex
	avg          map[exchange.Symbol]float64
	fired        map[exchange.Symbol]string  // symbol -> last fired date (YYYY-MM-DD)
	lastTurnover map[exchange.Symbol]float64 // symbol -> last seen intraday turnover
}

func NewVolumeStore() *VolumeStore {
	return &VolumeStore{
		avg:          make(map[exchange.Symbol]float64),
		fired:        make(map[exchange.Symbol]string),
		lastTurnover: make(map[exchange.Symbol]float64),
	}
}

// Set stores the 90-day average turnover for a symbol.
func (s *VolumeStore) Set(sym exchange.Symbol, avg float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.avg[sym] = avg
}

// Get returns the stored average and whether it exists.
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

// Delete removes a symbol from the store (called when a symbol is delisted).
func (s *VolumeStore) Delete(sym exchange.Symbol) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.avg, sym)
	delete(s.fired, sym)
	delete(s.lastTurnover, sym)
}

// TryFire atomically checks whether an alert has already been sent for (sym,
// date). Returns true and records the pair on first call; returns false on
// subsequent calls for the same pair.
func (s *VolumeStore) TryFire(sym exchange.Symbol, date string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fired[sym] == date {
		return false
	}
	s.fired[sym] = date
	return true
}
