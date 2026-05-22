package store

import (
	"sync"

	"volume_pump_checker/domain/market"
)

// VolumeStore is a thread-safe in-memory cache of per-symbol multi-period
// average turnover and last-seen intraday turnover.
type VolumeStore struct {
	mu           sync.RWMutex
	avgs         map[market.Symbol]map[int]float64
	lastTurnover map[market.Symbol]float64
}

func NewVolumeStore() *VolumeStore {
	return &VolumeStore{
		avgs:         make(map[market.Symbol]map[int]float64),
		lastTurnover: make(map[market.Symbol]float64),
	}
}

func (s *VolumeStore) SetAvg(sym market.Symbol, days int, avg float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.avgs[sym] == nil {
		s.avgs[sym] = make(map[int]float64)
	}
	s.avgs[sym][days] = avg
}

func (s *VolumeStore) GetAvg(sym market.Symbol, days int) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.avgs[sym]
	if !ok {
		return 0, false
	}
	v, ok := m[days]
	return v, ok
}

// SwapTurnover atomically replaces the stored turnover and returns the previous value.
// ok=false on first call for this symbol.
func (s *VolumeStore) SwapTurnover(sym market.Symbol, current float64) (prev float64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, ok = s.lastTurnover[sym]
	s.lastTurnover[sym] = current
	return
}

func (s *VolumeStore) Delete(sym market.Symbol) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.avgs, sym)
	delete(s.lastTurnover, sym)
}
