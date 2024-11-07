package com.github.dts.util;

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.LinkedBlockingQueue;

public class RateLimiterBlockingQueue<E> extends LinkedBlockingQueue<E> {
    private final RateLimiter concurrentRequestLimiter;

    /**
     * 构造方法
     *
     * @param capacity 队列容量
     * @param qpm      每分钟调用次数（QPM）
     */
    public RateLimiterBlockingQueue(int capacity, int qpm) {
        super(capacity);
        this.concurrentRequestLimiter = RateLimiter.create(Math.floor((double) qpm / 60D));
    }

    @Override
    public void put(E e) throws InterruptedException {
        concurrentRequestLimiter.acquire(1);
        super.put(e);
    }
}
