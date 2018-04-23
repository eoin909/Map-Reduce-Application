package sampleAssignment;

import java.util.Map;

public interface ReduceCallback<E, K, V> {
    void reduceDone(E e, Map<K, V> results);

	//void reduceDone(String word, Map<String, FileMatch> reducedMap);
}