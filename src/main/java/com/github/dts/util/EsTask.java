package com.github.dts.util;

import java.util.concurrent.CompletableFuture;

public class EsTask extends CompletableFuture<EsTaskResponse> {
    private final String id;
    private final long timestamp = System.currentTimeMillis();

    public EsTask(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
