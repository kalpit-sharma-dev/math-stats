#### math-stats
trying to create a similar library in golang just like python to calculate descriptive stats for data

### v2
Performance Complexity
Mean, Min, Max: O(1)
Median: O(1) for maintenance, O(1) for retrieval.
Percentiles: O(log n) for insertion, O(1) for retrieval with pre-sorted slice.
