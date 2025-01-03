package com.github.dts.util;

import java.util.Collection;

public interface ESBulkRequest {

    ESBulkRequest add(Collection<ESRequest> requests, BulkPriorityEnum bulkPriority);

    ESBulkRequest add(ESUpdateByQueryRequest esUpdateRequest);

    ESBulkRequest add(ESUpdateRequest esUpdateRequest);

    ESBulkRequest add(ESDeleteRequest esDeleteRequest);

    int numberOfActions();

    boolean isEmpty();

    ESBulkResponse bulk();

    interface ESRequest extends TrimRequest {
        default void beforeBulk() {
        }
    }

    interface ESUpdateByQueryRequest extends ESRequest {

        int size();
    }

    interface ESUpdateRequest extends ESRequest {

    }

    interface ESDeleteRequest extends ESRequest {
    }

    interface EsRefreshResponse {
        String[] getIndices();
    }

    interface ESBulkResponse {
        boolean hasFailures();

        boolean isEmpty();

        void processFailBulkResponse(String errorMsg) throws RuntimeException;

        int size();

        long requestTotalEstimatedSizeInBytes();

        long requestEstimatedSizeInBytes();

        String[] requestBytesToString();
    }
}
