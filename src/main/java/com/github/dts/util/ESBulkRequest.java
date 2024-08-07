package com.github.dts.util;

import org.elasticsearch.script.Script;

import java.util.Collection;
import java.util.Map;

public interface ESBulkRequest {

    ESBulkRequest add(Collection<ESRequest> requests, BulkPriorityEnum bulkPriority);

    ESBulkRequest add(ESIndexRequest esIndexRequest);

    ESBulkRequest add(ESUpdateByQueryRequest esUpdateRequest);

    ESBulkRequest add(ESUpdateRequest esUpdateRequest);

    ESBulkRequest add(ESDeleteRequest esDeleteRequest);

    int numberOfActions();

    boolean isEmpty();

    ESBulkResponse bulk();

    interface ESIndexRequest extends ESRequest {

        ESIndexRequest setSource(Map<String, ?> source);

        ESIndexRequest setRouting(String routing);
    }

    interface ESUpdateByQueryRequest extends ESRequest {

        int size();
    }

    interface ESUpdateRequest extends ESRequest {

        ESUpdateRequest setDoc(Map source);

        ESUpdateRequest setDocAsUpsert(boolean shouldUpsertDoc);

        ESUpdateRequest setRouting(String routing);

        ESUpdateRequest setScript(Script script);
    }

    interface ESRequest {
    }

    interface ESDeleteRequest extends ESRequest {
    }

    interface EsRefreshResponse {

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
