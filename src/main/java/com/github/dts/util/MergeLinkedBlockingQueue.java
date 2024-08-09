package com.github.dts.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;

public class MergeLinkedBlockingQueue<E extends Runnable> extends LinkedBlockingQueue<E> {
    private final BiFunction<E, E, E> mergeFunction;

    public MergeLinkedBlockingQueue(int capacity, BiFunction<E, E, E> mergeFunction) {
        super(capacity);
        this.mergeFunction = mergeFunction;
    }

    @Override
    public boolean offer(E e) {
        if (mergeFunction == null) {
            return super.offer(e);
        }
        E poll = poll();
        if (poll == null) {
            return super.offer(e);
        } else {
            E merge = mergeFunction.apply(poll, e);
            if (merge == null) {
                super.offer(poll);
                return super.offer(e);
            } else {
                return super.offer(merge);
            }
        }
    }

}
