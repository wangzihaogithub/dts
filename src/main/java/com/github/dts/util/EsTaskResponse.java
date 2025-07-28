package com.github.dts.util;

import java.util.Map;

public class EsTaskResponse {
    private final Map<String, Object> responseBody;

    public EsTaskResponse(Map<String, Object> responseBody) {
        this.responseBody = responseBody;
    }

    public boolean isCompleted() {
        return Boolean.TRUE.equals(responseBody.get("completed"));
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
                "completed=" + isCompleted() +
                '}';
    }
}
