package com.github.dts.util;

import com.github.dts.util.ESSyncConfig.ESMapping;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ESTemplate extends AutoCloseable {

    /**
     * 插入数据
     *
     * @param mapping     配置对象
     * @param pkVal       主键值
     * @param esFieldData 数据Map
     * @param bulkRequestList bulkRequestList
     */
    void insert(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList);

    /**
     * 根据主键更新数据
     *
     * @param mapping     配置对象
     * @param pkVal       主键值
     * @param esFieldData 数据Map
     * @param bulkRequestList bulkRequestList
     */
    void update(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList);

    /**
     * update by query
     *
     * @param config      配置对象
     * @param paramsTmp   sql查询条件
     * @param esFieldData 数据Map
     * @param bulkRequestList bulkRequestList
     */
    void updateByQuery(ESSyncConfig config, Map<String, Object> paramsTmp, Map<String, Object> esFieldData, BulkRequestList bulkRequestList);

    /**
     * delete by range
     * @param mapping mapping
     * @param maxId maxId
     * @param minId minId
     * @return ESBulkResponse
     */
    ESBulkRequest.ESBulkResponse deleteByIdRange(ESMapping mapping, Integer minId, Integer maxId);

    ESBulkRequest.ESBulkResponse deleteByRange(ESMapping mapping, String fieldName, Object minValue, Object maxValue);

    /**
     * 通过主键删除数据
     *
     * @param mapping     配置对象
     * @param pkVal       主键值
     * @param esFieldData 数据Map
     * @param bulkRequestList bulkRequestList
     */
    void delete(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList);

    /**
     * 提交批次
     */
    void commit();

    /**
     * 刷盘
     * @param indices indices
     * @return 刷盘结果
     */
    CompletableFuture<ESBulkRequest.EsRefreshResponse> refresh(Collection<String> indices);

    int bulk(BulkRequestList bulkRequestList);

    Object getValFromRS(ESMapping mapping, ResultSet resultSet, String fieldName,
                        String columnName, Map<String, Object> data) throws SQLException;

    Object getESDataFromRS(ESMapping mapping, ResultSet resultSet, Map<String, Object> dmlOld,
                           Map<String, Object> esFieldData,
                           Map<String, Object> data) throws SQLException;

    Object getIdValFromRS(ESMapping mapping, ResultSet resultSet) throws SQLException;

    Object getValFromValue(ESMapping mapping, Object value, Map<String, Object> dmlData, String fieldName);

    Object getESDataFromRS(ESMapping mapping, ResultSet resultSet, Map<String, Object> dmlOld, Map<String, Object> esFieldData) throws SQLException;

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
