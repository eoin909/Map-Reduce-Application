package sampleAssignment;

import java.util.List;

public interface MapCallback<E, V> {
    void mapDone(E key, List<V> values);
}