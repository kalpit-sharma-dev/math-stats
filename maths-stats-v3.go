package main

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
	"sync"
)

// RingBuffer for storing recent data
type RingBuffer struct {
	data []float64
	head int
	size int
	cap  int
}

func NewRingBuffer(cap int) *RingBuffer {
	return &RingBuffer{
		data: make([]float64, cap),
		cap:  cap,
	}
}

func (rb *RingBuffer) Add(val float64) {
	rb.data[rb.head] = val
	rb.head = (rb.head + 1) % rb.cap
	if rb.size < rb.cap {
		rb.size++
	}
}

func (rb *RingBuffer) GetSorted() []float64 {
	sorted := append([]float64(nil), rb.data[:rb.size]...)
	sort.Float64s(sorted)
	return sorted
}

// DataStreamStats tracks streaming statistics
type DataStreamStats struct {
	minMaxLock      sync.Mutex
	heapLock        sync.Mutex
	percentileLock  sync.Mutex
	cachedLock      sync.Mutex
	totalSum        float64
	count           int64
	minVal          float64
	maxVal          float64
	lower           MaxHeap
	upper           MinHeap
	recentData      *RingBuffer
	balanceCounter  int
	cached          CachedStats
	cacheUpdated    bool
	cachePercentile map[int]float64
}

// CachedStats for quick read-heavy queries
type CachedStats struct {
	mean       float64
	median     float64
	percentile map[int]float64
}

// NewDataStreamStats initializes DataStreamStats
func NewDataStreamStats(capacity int) *DataStreamStats {
	return &DataStreamStats{
		minVal:          math.Inf(1),
		maxVal:          math.Inf(-1),
		lower:           MaxHeap{},
		upper:           MinHeap{},
		recentData:      NewRingBuffer(capacity),
		cachePercentile: make(map[int]float64),
	}
}

// AddNumber adds a number and updates statistics
func (ds *DataStreamStats) AddNumber(num float64) {
	ds.minMaxLock.Lock()
	defer ds.minMaxLock.Unlock()

	// Update basic stats
	ds.totalSum += num
	ds.count++
	if num < ds.minVal {
		ds.minVal = num
	}
	if num > ds.maxVal {
		ds.maxVal = num
	}

	// Maintain heaps
	ds.heapLock.Lock()
	if ds.lower.Len() == 0 || num <= ds.lower.Peek() {
		heap.Push(&ds.lower, num)
		ds.balanceCounter++
	} else {
		heap.Push(&ds.upper, num)
		ds.balanceCounter--
	}
	ds.balanceHeaps()
	ds.heapLock.Unlock()

	// Add to recent data (for percentiles)
	ds.percentileLock.Lock()
	ds.recentData.Add(num)
	ds.percentileLock.Unlock()

	// Invalidate cached stats
	ds.cachedLock.Lock()
	ds.cacheUpdated = false
	ds.cachedLock.Unlock()
}

// Balance heaps for median calculation
func (ds *DataStreamStats) balanceHeaps() {
	if ds.balanceCounter > 1 {
		heap.Push(&ds.upper, heap.Pop(&ds.lower))
		ds.balanceCounter--
	} else if ds.balanceCounter < -1 {
		heap.Push(&ds.lower, heap.Pop(&ds.upper))
		ds.balanceCounter++
	}
}

// GetMean calculates the mean
func (ds *DataStreamStats) GetMean() float64 {
	ds.minMaxLock.Lock()
	defer ds.minMaxLock.Unlock()

	if ds.count == 0 {
		return 0
	}
	return ds.totalSum / float64(ds.count)
}

// GetMedian calculates the median
func (ds *DataStreamStats) GetMedian() float64 {
	ds.heapLock.Lock()
	defer ds.heapLock.Unlock()

	if ds.count == 0 {
		return 0
	}
	if ds.lower.Len() > ds.upper.Len() {
		return ds.lower.Peek()
	}
	return (ds.lower.Peek() + ds.upper.Peek()) / 2
}

// GetMin returns the minimum value
func (ds *DataStreamStats) GetMin() float64 {
	ds.minMaxLock.Lock()
	defer ds.minMaxLock.Unlock()
	return ds.minVal
}

// GetMax returns the maximum value
func (ds *DataStreamStats) GetMax() float64 {
	ds.minMaxLock.Lock()
	defer ds.minMaxLock.Unlock()
	return ds.maxVal
}

// GetPercentile calculates a given percentile
func (ds *DataStreamStats) GetPercentile(p float64) float64 {
	ds.percentileLock.Lock()
	defer ds.percentileLock.Unlock()

	sorted := ds.recentData.GetSorted()
	if len(sorted) == 0 {
		return 0
	}

	index := int(math.Ceil((p / 100) * float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}

// GetCachedStats returns cached stats if available
func (ds *DataStreamStats) GetCachedStats() CachedStats {
	ds.cachedLock.Lock()
	defer ds.cachedLock.Unlock()

	if ds.cacheUpdated {
		return ds.cached
	}

	ds.cached.mean = ds.GetMean()
	ds.cached.median = ds.GetMedian()
	ds.cached.percentile[95] = ds.GetPercentile(95)
	ds.cached.percentile[99] = ds.GetPercentile(99)
	ds.cacheUpdated = true

	return ds.cached
}

// MinHeap is a min-heap
type MinHeap []float64

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *MinHeap) Peek() float64     { return h[0] }
func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(float64))
}
func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MaxHeap is a max-heap
type MaxHeap []float64

func (h MaxHeap) Len() int           { return len(h) }
func (h MaxHeap) Less(i, j int) bool { return h[i] > h[j] }
func (h *MaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *MaxHeap) Peek() float64      { return h[0] }
func (h *MaxHeap) Push(x interface{}) {
	*h = append(*h, x.(float64))
}
func (h *MaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func main() {
	stats := NewDataStreamStats(1000) // Ring buffer for last 1000 elements

	// Simulated data stream
	for i := 1; i <= 1000000; i++ {
		stats.AddNumber(float64(i))
	}

	// Print cached statistics
	cachedStats := stats.GetCachedStats()
	fmt.Printf("Mean: %.2f\n", cachedStats.mean)
	fmt.Printf("Min: %.2f\n", stats.GetMin())
	fmt.Printf("Max: %.2f\n", stats.GetMax())
	fmt.Printf("Median: %.2f\n", cachedStats.median)
	fmt.Printf("95th Percentile: %.2f\n", cachedStats.percentile[95])
	fmt.Printf("99th Percentile: %.2f\n", cachedStats.percentile[99])
}
