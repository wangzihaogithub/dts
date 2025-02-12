package com.github.dts.util;

import org.elasticsearch.rest.RestStatus;

import java.util.Collection;
import java.util.List;

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

        public List<Failure> getFailureNotFoundList();

        boolean isEmpty();

        void processFailBulkResponse(String errorMsg) throws RuntimeException;

        int size();

        long requestTotalEstimatedSizeInBytes();

        long requestEstimatedSizeInBytes();

        String[] requestBytesToString();
    }

    public static class Failure {
        public final String index;
        public final String type;
        public final String id;
        public final Exception cause;
        public final RestStatus status;
        public final long seqNo;
        public final long term;
        public final boolean aborted;

        public Failure(String index, String type, String id, Exception cause, RestStatus status, long seqNo, long term, boolean aborted) {
            this.index = index;
            this.type = type;
            this.id = id;
            this.cause = cause;
            this.status = status;
            this.seqNo = seqNo;
            this.term = term;
            this.aborted = aborted;
        }
    }
}
