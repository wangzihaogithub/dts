package com.github.dts.util;

import java.util.Map;

public class EsActionResponse {
    private final Map<String, Object> responseBody;
    private final String endpoint;

    public EsActionResponse(String endpoint, Map<String, Object> responseBody) {
        this.endpoint = endpoint;
        this.responseBody = responseBody;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public boolean isSuccess() {
        return !responseBody.containsKey("error");
    }

    public boolean isAcknowledged() {
        return Boolean.TRUE.equals(responseBody.get("acknowledged"));
    }

    public Integer getStatus() {
        return (Integer) responseBody.get("status");
    }

    public Map<String, Object> getError() {
        return (Map<String, Object>) responseBody.get("error");
    }

    public Map<String, Object> getResponseBody() {
        return responseBody;
    }

    @Override
    public String toString() {
        return "EsActionResponse{" +
                "endpoint=" + endpoint +
                ", success=" + isSuccess() +
                '}';
    }
}
