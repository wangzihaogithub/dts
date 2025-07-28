package com.github.dts.util;

import java.util.Map;

public class EsTaskResponse {
    private final String taskId;
    private final Map<String, Object> responseBody;

    public EsTaskResponse(String taskId, Map<String, Object> responseBody) {
        this.taskId = taskId;
        this.responseBody = responseBody;
    }

    public boolean isCompleted() {
        return Boolean.TRUE.equals(responseBody.get("completed"));
    }

    public String getTaskId() {
        return taskId;
    }

    public Map<String, Object> getTask() {
        return (Map<String, Object>) responseBody.get("task");
    }

    public Map<String, Object> getResponseBody() {
        return responseBody;
    }

    @Override
    public String toString() {
        return "EsTaskResponse{" +
                "taskId=" + taskId +
                ",completed=" + isCompleted() +
                '}';
    }
}
