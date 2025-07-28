package com.github.dts.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Lists {

    public static <T> List<List<T>> partition(Collection<T> list, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Size must be greater than 0");
        }
        if (list == null || list.isEmpty()) {
            return new ArrayList<>();
        }
        int totalSize = list.size();
        int numPartitions = (int) Math.ceil((double) totalSize / size);

        return IntStream.range(0, numPartitions)
                .mapToObj(i -> new ArrayList<>(list.stream()
                        .skip(i * size)
                        .limit(size)
                        .collect(Collectors.toList())))
                .collect(Collectors.toList());
    }

}
