package com.github.dts.util;

import java.util.concurrent.CompletableFuture;

public class EsTaskCompletableFuture<T> extends CompletableFuture<T> {
    private String taskId;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
}
