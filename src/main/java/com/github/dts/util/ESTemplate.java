package com.github.dts.util;

import com.github.dts.util.ESSyncConfig.ESMapping;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public interface ESTemplate extends AutoCloseable {

    static CommitListener merge(CommitListener listener1, CommitListener listener2) {
        if (listener1 == listener2) {
            return listener1;
        }
        if (listener1 != null && listener2 != null) {
            return response -> {
                listener1.done(response);
                listener2.done(response);
            };
        }
        return listener1 != null ? listener1 : listener2;
    }

    /**
     * 插入数据
     *
     * @param mapping         配置对象
     * @param pkVal           主键值
     * @param esFieldData     数据Map
     * @param bulkRequestList bulkRequestList
     * @return ESBulkResponse
     */
    ESBulkRequest.ESBulkResponse insert(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList);

    /**
     * 根据主键更新数据
     *
     * @param mapping         配置对象
     * @param pkVal           主键值
     * @param esFieldData     数据Map
     * @param bulkRequestList bulkRequestList
     */
    void update(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList);

    void update(ESMapping mapping, String parentFieldName, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList);

    /**
     * update by query
     *
     * @param config          配置对象
     * @param paramsTmp       sql查询条件
     * @param esFieldData     数据Map
     * @param bulkRequestList bulkRequestList
     */
    void updateByQuery(ESSyncConfig config, Map<String, Object> paramsTmp, Map<String, Object> esFieldData, BulkRequestList bulkRequestList);

    /**
     * delete by range
     *
     * @param mapping mapping
     * @param maxId   maxId
     * @param minId   minId
     * @return ESBulkResponse
     */
    ESBulkRequest.ESBulkResponse deleteByIdRange(ESMapping mapping, String minId, String maxId);

    ESBulkRequest.ESBulkResponse deleteByRange(ESMapping mapping, String fieldName, Object minValue, Object maxValue, Integer limit);

    ESSearchResponse searchAfter(ESMapping mapping, String[] includes, String[] excludes, Object[] searchAfter, Integer limit);

    ESSearchResponse searchAfterId(ESMapping mapping, Object[] searchAfter, Integer limit);

    Set<String> searchByIds(ESMapping mapping, List<?> ids);

    /**
     * 通过主键删除数据
     *
     * @param mapping         配置对象
     * @param pkVal           主键值
     * @param bulkRequestList bulkRequestList
     * @return ESBulkResponse
     */
    ESBulkRequest.ESBulkResponse delete(ESMapping mapping, Object pkVal, BulkRequestList bulkRequestList);

    /**
     * 提交批次
     *
     * @return ESBulkResponse
     */
    ESBulkRequest.ESBulkResponse commit();

    /**
     * 刷盘
     *
     * @param indices indices
     * @return 刷盘结果
     */
    CompletableFuture<ESBulkRequest.EsRefreshResponse> refresh(Collection<String> indices);

    int bulk(BulkRequestList bulkRequestList);

    BulkRequestList newBulkRequestList(BulkPriorityEnum priorityEnum);

    ESFieldTypesCache getEsType(ESMapping mapping);

    interface BulkRequestList {
        ESBulkRequest.ESBulkResponse add(ESBulkRequest.ESRequest request);

        boolean isEmpty();

        int size();

        default ESBulkRequest.ESBulkResponse commit(ESTemplate esTemplate) {
            return commit(esTemplate, null);
        }

        ESBulkRequest.ESBulkResponse commit(ESTemplate esTemplate, CommitListener listener);

        BulkRequestList fork(BulkRequestList bulkRequestList);

        BulkRequestList fork(BulkPriorityEnum priorityEnum);
    }

    interface CommitListener {
        void done(ESBulkRequest.ESBulkResponse response);
    }

    interface RefreshListener {
        void done(ESBulkRequest.EsRefreshResponse response);
    }

    class ESSearchResponse {
        private final List<Hit> hitList = new ArrayList<>();

        public List<Hit> getHitList() {
            return hitList;
        }

        public Object[] getLastSortValues() {
            if (hitList.isEmpty()) {
                return null;
            }
            Hit hit = hitList.get(hitList.size() - 1);
            return hit.getSortValues();
        }
    }

    static class Hit extends LinkedHashMap<String, Object> {
        private final String id;
        private final Object[] sortValues;

        public Hit(String id, Map<String, Object> map, Object[] sortValues) {
            super(map);
            this.id = id;
            this.sortValues = sortValues;
        }

        public String getId() {
            return id;
        }

        public Object[] getSortValues() {
            return sortValues;
        }

        @Override
        public String toString() {
            return id;
        }
    }
}
