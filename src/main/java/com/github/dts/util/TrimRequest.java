package com.github.dts.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public interface TrimRequest {
    public static void trim(LinkedList<? extends TrimRequest> requests) {
        TrimRequest[] snapshotList = new TrimRequest[requests.size()];
        requests.toArray(snapshotList);
        List<TrimRequest> overlapList = new ArrayList<>(2);
        for (int i = snapshotList.length - 1; i >= 0; i--) {
            TrimRequest request = snapshotList[i];

            // collectOverlap
            for (TrimRequest row : snapshotList) {
                if (row == null) {
                    break;
                }
                if (row == request) {
                    break;
                }
                if (request.isOverlap(row)) {
                    overlapList.add(row);
                }
            }

            if (!overlapList.isEmpty()) {
                requests.removeIf(e -> {
                    for (TrimRequest esRequest : overlapList) {
                        if (e == esRequest) {
                            return true;
                        }
                    }
                    return false;
                });
                i -= overlapList.size();
                requests.toArray(snapshotList);
                overlapList.clear();
            }
        }
    }

    public static void merge(LinkedList<? extends TrimRequest> requests) {
        Iterator<? extends TrimRequest> iterator = requests.iterator();
        TrimRequest prev;
        if (iterator.hasNext()) {
            prev = iterator.next();
        } else {
            return;
        }
        while (iterator.hasNext()) {
            TrimRequest next = iterator.next();
            if (prev.mergeTo(next)) {
                iterator.remove();
            } else {
                prev = next;
            }
        }
    }

    default boolean isOverlap(TrimRequest prev) {
        return false;
    }

    default <T extends TrimRequest> boolean mergeTo(T next) {
        return false;
    }
}
