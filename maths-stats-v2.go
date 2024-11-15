package main

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
	"sync"
)

// DataStreamStats holds statistics for the data stream
type DataStreamStats struct {
	mu       sync.Mutex
	totalSum float64
	count    int64
	minVal   float64
	maxVal   float64
	lower    MaxHeap // Max-heap for lower half
	upper    MinHeap // Min-heap for upper half
	sorted   []float64
}

// NewDataStreamStats initializes a new instance of DataStreamStats
func NewDataStreamStats() *DataStreamStats {
	return &DataStreamStats{
		minVal: math.Inf(1),
		maxVal: math.Inf(-1),
		lower:  MaxHeap{},
		upper:  MinHeap{},
	}
}

// AddNumber adds a number to the data stream and updates statistics
func (ds *DataStreamStats) AddNumber(num float64) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Update basic statistics
	ds.totalSum += num
	ds.count++
	if num < ds.minVal {
		ds.minVal = num
	}
	if num > ds.maxVal {
		ds.maxVal = num
	}

	// Maintain heaps for median
	if ds.lower.Len() == 0 || num <= ds.lower.Peek() {
		heap.Push(&ds.lower, num)
	} else {
		heap.Push(&ds.upper, num)
	}

	// Balance heaps
	if ds.lower.Len() > ds.upper.Len()+1 {
		heap.Push(&ds.upper, heap.Pop(&ds.lower))
	} else if ds.upper.Len() > ds.lower.Len() {
		heap.Push(&ds.lower, heap.Pop(&ds.upper))
	}

	// Insert number into sorted slice using binary insertion
	idx := sort.SearchFloat64s(ds.sorted, num)
	ds.sorted = append(ds.sorted[:idx], append([]float64{num}, ds.sorted[idx:]...)...)
}

// GetMean calculates the mean of the stream
func (ds *DataStreamStats) GetMean() float64 {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.count == 0 {
		return 0
	}
	return ds.totalSum / float64(ds.count)
}

// GetMedian calculates the median of the stream
func (ds *DataStreamStats) GetMedian() float64 {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.count == 0 {
		return 0
	}
	if ds.lower.Len() > ds.upper.Len() {
		return ds.lower.Peek()
	}
	return (ds.lower.Peek() + ds.upper.Peek()) / 2
}

// GetMin returns the minimum value in the stream
func (ds *DataStreamStats) GetMin() float64 {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.minVal
}

// GetMax returns the maximum value in the stream
func (ds *DataStreamStats) GetMax() float64 {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.maxVal
}

// GetPercentile calculates the given percentile (e.g., 95th, 99th)
func (ds *DataStreamStats) GetPercentile(p float64) float64 {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if len(ds.sorted) == 0 {
		return 0
	}

	index := int(math.Ceil((p / 100) * float64(len(ds.sorted)))) - 1
	if index < 0 {
		index = 0
	}
	return ds.sorted[index]
}

// MinHeap is a min-heap of float64
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

// MaxHeap is a max-heap of float64
type MaxHeap []float64

func (h MaxHeap) Len() int           { return len(h) }
func (h MaxHeap) Less(i, j int) bool { return h[i] > h[j] } // Reverse for max-heap
func (h MaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
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
	// Simulated data stream
	dataChan := make(chan float64, 1000)
	doneChan := make(chan bool)

	stats := NewDataStreamStats()

	// Start the worker to process the stream
	go func() {
		for num := range dataChan {
			stats.AddNumber(num)
		}
		doneChan <- true
	}()

	// Simulate adding data to the stream
	go func() {
		for i := 1; i <= 1000000; i++ {
			dataChan <- float64(i)
		}
		close(dataChan)
	}()

	// Wait for the processing to complete
	<-doneChan

	// Print statistics
	fmt.Printf("Mean: %.2f\n", stats.GetMean())
	fmt.Printf("Min: %.2f\n", stats.GetMin())
	fmt.Printf("Max: %.2f\n", stats.GetMax())
	fmt.Printf("Median: %.2f\n", stats.GetMedian())
	fmt.Printf("95th Percentile: %.2f\n", stats.GetPercentile(95))
	fmt.Printf("99th Percentile: %.2f\n", stats.GetPercentile(99))
}
