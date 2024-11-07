package com.github.dts.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class VectorCompletableFuture extends CompletableFuture<float[]> {
    private static final Map<String, RateLimiterBlockingQueue<VectorCompletableFuture>> FUTURE_LIST_MAP = new ConcurrentHashMap<>(3);

    private final String content;
    private final Runnable beforeGet;

    public VectorCompletableFuture(String content, Runnable beforeGet) {
        this.content = content;
        this.beforeGet = beforeGet;
    }

    public static RateLimiterBlockingQueue<VectorCompletableFuture> getQueue(String name, int requestMaxContentSize, int qpm) {
        return FUTURE_LIST_MAP.computeIfAbsent(name, k -> new RateLimiterBlockingQueue<>(Math.max(1, requestMaxContentSize), qpm));
    }

    public static void beforeBulk(Object params) {
        List<Object> stack = new ArrayList<>();
        stack.add(params);
        while (!stack.isEmpty()) {
            Object remove = stack.remove(stack.size() - 1);
            if (remove instanceof Map) {
                for (Map.Entry<String, Object> entry : ((Map<String, Object>) remove).entrySet()) {
                    Object value = entry.getValue();
                    if (isCompletableFuture(value)) {
                        entry.setValue(getFuture(value));
                    } else {
                        stack.add(value);
                    }
                }
            } else if (remove instanceof List) {
                List list = (List<?>) remove;
                int i = 0;
                for (Object item : list) {
                    if (isCompletableFuture(item)) {
                        list.set(i, getFuture(item));
                    } else {
                        stack.add(item);
                    }
                }
            }
        }
    }

    private static boolean isCompletableFuture(Object data) {
        return data instanceof CompletableFuture;
    }

    private static Object getFuture(Object data) {
        try {
            return ((CompletableFuture<?>) data).get();
        } catch (Exception e) {
            Util.sneakyThrows(e);
            return null;
        }
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