package io.zulia.server.search.aggregation.facets;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

public class SegmentedIntIntMap {

    private final int numSegments;
    private final IntIntMap[] segments;

    public SegmentedIntIntMap(int numSegments) {
        this.numSegments = numSegments;
        this.segments = new IntIntMap[numSegments];
        for (int i = 0; i < numSegments; i++) {
            segments[i] = HashIntIntMaps.newMutableMap();
        }
    }

    private int getSegmentIndex(int key) {
        return Math.abs(Integer.hashCode(key) % numSegments);
    }

    public void put(int key, int value) {
        int segmentIndex = getSegmentIndex(key);
        synchronized (segments[segmentIndex]) {
            segments[segmentIndex].put(key, value);
        }
    }

    public void increment(int key) {
        int segmentIndex = getSegmentIndex(key);
        synchronized (segments[segmentIndex]) {
            segments[segmentIndex].addValue(key, 1);
        }
    }

    public int get(int key) {
        int segmentIndex = getSegmentIndex(key);
        synchronized (segments[segmentIndex]) {
            return segments[segmentIndex].get(key);
        }
    }

}