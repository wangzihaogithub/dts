package com.github.dts.util;

import org.elasticsearch.script.Script;

import java.util.Collection;
import java.util.Map;

public interface ESBulkRequest {

    ESBulkRequest add(Collection<ESRequest> requests);

    ESBulkRequest add(ESIndexRequest esIndexRequest);

    ESBulkRequest add(ESUpdateRequest esUpdateRequest);

    ESBulkRequest add(ESDeleteRequest esDeleteRequest);

    int numberOfActions();

    boolean isEmpty();

    ESBulkResponse bulk();

    interface ESIndexRequest extends ESRequest {

        ESIndexRequest setSource(Map<String, ?> source);

        ESIndexRequest setRouting(String routing);
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

    interface EsRefreshResponse{

    }
    interface ESBulkResponse {
        boolean hasFailures();

        boolean isEmpty();

        void processFailBulkResponse(String errorMsg);

        int size();

    }
}
