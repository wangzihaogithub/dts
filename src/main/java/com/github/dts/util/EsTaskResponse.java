package com.github.dts.util;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class EsTaskResponse extends EsActionResponse {
    private final String taskId;

    public EsTaskResponse(String taskId, Map<String, Object> responseBody) {
        super("GET /_tasks/" + taskId, responseBody);
        this.taskId = taskId;
    }

    public boolean isCompleted() {
        return Boolean.TRUE.equals(getResponseBody().get("completed"));
    }

    public String getTaskId() {
        return taskId;
    }

    public String getTaskDescription() {
        return Optional.ofNullable(getTask()).map(e -> Objects.toString(e.get("description"), null)).orElse(null);
    }

    public Map<String, Object> getTask() {
        return (Map<String, Object>) getResponseBody().get("task");
    }

    @Override
    public String toString() {
        return "EsTaskResponse{" +
                "taskId=" + taskId +
                ", description=" + getTaskDescription() +
                ", completed=" + isCompleted() +
                '}';
    }
}
