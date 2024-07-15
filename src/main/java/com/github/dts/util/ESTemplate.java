package com.github.dts.util;

import com.github.dts.util.ESSyncConfig.ESMapping;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ESTemplate extends AutoCloseable {

    /**
     * 插入数据
     *
     * @param mapping         配置对象
     * @param pkVal           主键值
     * @param esFieldData     数据Map
     * @param bulkRequestList bulkRequestList
     */
    void insert(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList);

    /**
     * 根据主键更新数据
     *
     * @param mapping         配置对象
     * @param pkVal           主键值
     * @param esFieldData     数据Map
     * @param bulkRequestList bulkRequestList
     */
    void update(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList);

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

    /**
     * 通过主键删除数据
     *
     * @param mapping         配置对象
     * @param pkVal           主键值
     * @param esFieldData     数据Map
     * @param bulkRequestList bulkRequestList
     */
    void delete(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList);

    /**
     * 提交批次
     */
    void commit();

    /**
     * 刷盘
     *
     * @param indices indices
     * @return 刷盘结果
     */
    CompletableFuture<ESBulkRequest.EsRefreshResponse> refresh(Collection<String> indices);

    int bulk(BulkRequestList bulkRequestList);

    Object getValFromRS(ESMapping mapping, Map<String, Object> row, String fieldName,
                        String columnName, Map<String, Object> data);

    Object getESDataFromRS(ESMapping mapping, Map<String, Object> row, Map<String, Object> dmlOld,
                           Map<String, Object> esFieldData,
                           Map<String, Object> data);


    Object getESDataFromRS(ESMapping mapping, Map<String, Object> row,
                           Map<String, Object> esFieldData, Map<String, Object> data);

    Object getIdValFromRS(ESMapping mapping, Map<String, Object> row);

    /**
     * 转换类型
     *
     * @param esMapping     es映射关系
     * @param theConvertMap 需要转换的数据
     */
    void convertValueType(ESMapping esMapping,String pfieldName,
                          Map<String, Object> theConvertMap);


    Object getValFromData(ESMapping mapping, Map<String, Object> dmlData, String fieldName, String columnName);

    Object getESDataFromDmlData(ESMapping mapping, Map<String, Object> dmlData,
                                Map<String, Object> esFieldData);

    Object getESDataFromDmlData(ESMapping mapping, Map<String, Object> dmlData, Map<String, Object> dmlOld,
                                Map<String, Object> esFieldData, String tableName);

    void updateByScript(ESMapping mapping, String idOrCode, boolean isUpsert, Object pkValue, int scriptTypeId, String lang, Map<String, Object> params, BulkRequestList bulkRequestList);

    void updateByQuery(ESMapping mapping, Map<String, Object> esFieldDataWhere, Map<String, Object> esFieldData, BulkRequestList bulkRequestList);

    BulkRequestList newBulkRequestList();

    interface BulkRequestList {
        void add(ESBulkRequest.ESRequest request);

        boolean isEmpty();

        int size();
    }
}
