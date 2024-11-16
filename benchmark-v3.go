package main

import (
	"math/rand"
	"testing"
)

func BenchmarkAddNumber(b *testing.B) {
	stats := NewDataStreamStats(1000) // Ring buffer for last 1000 elements

	// Benchmark adding numbers
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats.AddNumber(rand.Float64() * 1000)
	}
}

func BenchmarkGetMean(b *testing.B) {
	stats := NewDataStreamStats(1000)

	// Prepopulate with random data
	for i := 0; i < 1000; i++ {
		stats.AddNumber(rand.Float64() * 1000)
	}

	// Benchmark getting the mean
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats.GetMean()
	}
}

func BenchmarkGetMedian(b *testing.B) {
	stats := NewDataStreamStats(1000)

	// Prepopulate with random data
	for i := 0; i < 1000; i++ {
		stats.AddNumber(rand.Float64() * 1000)
	}

	// Benchmark getting the median
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats.GetMedian()
	}
}

func BenchmarkGetPercentile(b *testing.B) {
	stats := NewDataStreamStats(1000)

	// Prepopulate with random data
	for i := 0; i < 1000; i++ {
		stats.AddNumber(rand.Float64() * 1000)
	}

	// Benchmark getting the 95th percentile
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats.GetPercentile(95)
	}
}
