package com.github.dts.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class VectorCompletableFuture extends CompletableFuture<float[]> {
    private final String content;
    private final Runnable beforeGet;

    public VectorCompletableFuture(String content, Runnable beforeGet) {
        this.content = content;
        this.beforeGet = beforeGet;
    }

    public String getContent() {
        return content;
    }

    @Override
    public float[] get() throws InterruptedException, ExecutionException {
        if (!isDone()) {
            this.beforeGet.run();
        }
        return super.get();
    }

    @Override
    public float[] get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!isDone()) {
            this.beforeGet.run();
        }
        return super.get(timeout, unit);
    }

    @Override
    public float[] getNow(float[] valueIfAbsent) {
        if (!isDone()) {
            this.beforeGet.run();
        }
        return super.getNow(valueIfAbsent);
    }
}