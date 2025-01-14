package com.github.dts.util;

import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.SchemaItem.FieldItem;
import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * ES 操作模板
 */
public class DefaultESTemplate implements ESTemplate {
    private static final Logger logger = LoggerFactory.getLogger(DefaultESTemplate.class);
    private static final ConcurrentMap<String, ESFieldTypesCache> esFieldTypes = new ConcurrentHashMap<>(2);
    private final ESConnection esConnection;
    private final ESConnection.ESBulkRequest esBulkRequest;
    private final BulkRequestListAddAfter bulkRequestListAddAfter = new BulkRequestListAddAfter();
    private int deleteByIdRangeBatch = 1000;

    public DefaultESTemplate(ESConnection esConnection) {
        this.esConnection = esConnection;
        this.esBulkRequest = new ESConnection.ESBulkRequest(esConnection);
    }

    private static boolean isInteger(String id) {
        if (id.length() > String.valueOf(Integer.MAX_VALUE).length()) {
            return false;
        }
        try {
            int i = Integer.parseInt(id);
            return Integer.toString(i).equals(id);
        } catch (Exception e) {
            return false;
        }
    }

    private static Object sqlRS(DataSource ds, String sql, Function<ResultSet, Object> fun) {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);) {
            stmt.setFetchSize(Integer.MIN_VALUE);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                return fun.apply(rs);
            }
        } catch (Exception e) {
            logger.error("sqlRs has error, sql: {} ,异常信息：{}", sql, e.toString());
            Util.sneakyThrows(e);
            return null;
        }
    }

    private static void trimRemoveIndexUpdateTime(ESMapping mapping, BulkRequestList bulkRequestList, Map<String, Object> esFieldData, String index, String pkToString) {
        if (mapping.isSetIndexUpdatedTime() && bulkRequestList instanceof BulkRequestListImpl) {
            if (((BulkRequestListImpl) bulkRequestList).containsUpdate(index, pkToString)) {
                esFieldData.remove(mapping.getIndexUpdatedTime());
            }
        }
    }

    private boolean isMaxBatchSize(int size) {
        return esConnection.isMaxBatchSize(size);
    }

    /**
     * 插入数据
     *
     * @param mapping     配置对象
     * @param pkVal       主键值
     * @param esFieldData 数据Map
     */
    @Override
    public void insert(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList) {
        if (pkVal == null || "".equals(pkVal)) {
            return;
        }
        setterIndexUpdatedTime(mapping, esFieldData);

        esFieldData = copyAndConvertType(esFieldData, mapping);
        ESBulkRequest.ESUpdateRequest updateRequest = new ESConnection.ESUpdateRequestImpl(mapping.get_index(),
                pkVal.toString(), esFieldData, true, mapping.getRetryOnConflict());
        addRequest(updateRequest, bulkRequestList);
    }

    /**
     * 根据主键更新数据
     *
     * @param mapping     配置对象
     * @param pkVal       主键值
     * @param esFieldData 数据Map
     */

    @Override
    public void update(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList) {
        update(mapping, null, pkVal, esFieldData, bulkRequestList);
    }

    @Override
    public void update(ESMapping mapping, String parentFieldName, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList) {
        if (pkVal == null || "".equals(pkVal)) {
            return;
        }
        if (esFieldData.isEmpty()) {
            return;
        }
        Map<String, Object> esFieldDataTmp = new LinkedHashMap<>();
        setterIndexUpdatedTime(mapping, esFieldDataTmp);
        Map<String, Object> esFieldDataConvert;
        if (parentFieldName == null) {
            esFieldDataTmp.putAll(esFieldData);
            esFieldDataConvert = copyAndConvertType(esFieldDataTmp, mapping);
        } else {
            esFieldDataConvert = copyAndConvertType(esFieldDataTmp, mapping);
            if (esFieldDataConvert != null) {
                esFieldDataConvert = new LinkedHashMap<>(esFieldDataConvert);
                esFieldDataConvert.putAll(esFieldData);
            }
        }
        append4Update(mapping, pkVal, esFieldDataConvert, bulkRequestList);
    }

    private void setterIndexUpdatedTime(ESMapping mapping, Map<String, Object> esFieldData) {
        if (mapping.isSetIndexUpdatedTime()) {
            esFieldData.put(mapping.getIndexUpdatedTime(), new Timestamp(System.currentTimeMillis()));
        }
    }

    @Override
    public BulkRequestListImpl newBulkRequestList(BulkPriorityEnum priorityEnum) {
        return new BulkRequestListImpl(bulkRequestListAddAfter, priorityEnum, new ConcurrentHashMap<>());
    }

    /**
     * update by query
     *
     * @param config      配置对象
     * @param paramsTmp   sql查询条件
     * @param esFieldData 数据Map
     */
    @Override
    public void updateByQuery(ESSyncConfig config, Map<String, Object> paramsTmp, Map<String, Object> esFieldData, BulkRequestList bulkRequestList) {
        if (paramsTmp.isEmpty()) {
            return;
        }
        if (esFieldData.isEmpty()) {
            return;
        }
        if (paramsTmp.containsKey(ESSyncConfig.ES_ID_FIELD_NAME)) {
            update(config.getEsMapping(), paramsTmp.get(ESSyncConfig.ES_ID_FIELD_NAME), esFieldData, bulkRequestList);
        } else {
            ESMapping mapping = config.getEsMapping();
            setterIndexUpdatedTime(mapping, esFieldData);

            Map<String, Object> esFieldDataTmp = copyAndConvertType(esFieldData, mapping);

            // 查询sql批量更新
            DataSource ds = CanalConfig.DatasourceConfig.getDataSource(config.getDataSourceKey());
            StringBuilder sql = new StringBuilder("SELECT * FROM (" + mapping.getSql() + ") _v WHERE ");
            paramsTmp.forEach(
                    (fieldName, value) -> sql.append("_v.").append(fieldName).append("=")
                            .append(value instanceof Number ? value : "'" + value + "'")
                            .append(" AND "));
            int len = sql.length();
            sql.delete(len - 4, len);
            Integer syncCount = (Integer) sqlRS(ds, sql.toString(), rs -> {
                int count = 0;
                try {
                    while (rs.next()) {
                        Object idVal = getIdValFromRS(mapping, rs);
                        append4Update(mapping, idVal, new LinkedHashMap<>(esFieldDataTmp), bulkRequestList);
                        count++;
                    }
                } catch (Exception e) {
                    Util.sneakyThrows(e);
                    return null;
                }
                return count;
            });
            if (logger.isTraceEnabled()) {
                logger.trace("Update ES by query affected {} records", syncCount);
            }
        }
    }

    public int getDeleteByIdRangeBatch() {
        return deleteByIdRangeBatch;
    }

    public void setDeleteByIdRangeBatch(int deleteByIdRangeBatch) {
        this.deleteByIdRangeBatch = deleteByIdRangeBatch;
    }

    @Override
    public ESBulkRequest.ESBulkResponse deleteByIdRange(ESMapping mapping, String minIdString, String maxIdString) {
        if (minIdString == null || maxIdString == null) {
            return null;
        }
        String idColName = mapping.get_id();
        if (idColName == null) {
            return null;
        }
        String indexName = mapping.get_index();
        int add = deleteByIdRangeBatch;
        ESConnection.ESBulkRequest bulkRequest = new ESConnection.ESBulkRequest(esConnection);
        if (isInteger(minIdString) && isInteger(maxIdString)) {
            int minId = Integer.parseInt(minIdString);
            int maxId = Integer.parseInt(maxIdString);
            for (int i = minId + add, slot = minId; i <= maxId; slot = i, i += add) {
                ESConnection.ESSearchRequest esSearchRequest = new ESConnection.ESSearchRequest(indexName)
                        .setQuery(QueryBuilders.rangeQuery(idColName).lte(i).gte(Math.max(slot, minId)))
                        .fetchSource(idColName)
                        .size(add);
                SearchResponse response = esSearchRequest.getResponse(this.esConnection);
                for (SearchHit hit : response.getHits()) {
                    bulkRequest.add(new ESConnection.ESDeleteRequestImpl(indexName, hit.getId()));
                    if (bulkRequest.numberOfActions() > add) {
                        bulkRequest.bulk();
                    }
                }
            }
        } else {
            ESConnection.ESSearchRequest esSearchRequest = new ESConnection.ESSearchRequest(indexName)
                    .setQuery(QueryBuilders.rangeQuery(idColName).lte(maxIdString).gte(minIdString))
                    .fetchSource(idColName);
            SearchResponse response = esSearchRequest.getResponse(this.esConnection);
            for (SearchHit hit : response.getHits()) {
                bulkRequest.add(new ESConnection.ESDeleteRequestImpl(indexName, hit.getId()));
                if (bulkRequest.numberOfActions() > add) {
                    bulkRequest.bulk();
                }
            }
        }
        return bulkRequest.bulk();
    }

    @Override
    public ESBulkRequest.ESBulkResponse deleteByRange(ESMapping mapping, String fieldName, Object minValue, Object maxValue, Integer limit) {
        if (fieldName == null || fieldName.isEmpty()) {
            return null;
        }
        if (maxValue == null && minValue == null) {
            return null;
        }
        String indexName = mapping.get_index();
        ESConnection.ESBulkRequest bulkRequest = new ESConnection.ESBulkRequest(esConnection);
        RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery(fieldName);
        if (maxValue != null) {
            queryBuilder.lte(maxValue);
        }
        if (minValue != null) {
            queryBuilder.gte(minValue);
        }
        ESConnection.ESSearchRequest esSearchRequest = new ESConnection.ESSearchRequest(indexName)
                .setQuery(queryBuilder)
                .fetchSource(fieldName);
        if (limit != null) {
            esSearchRequest.size(limit);
        }
        SearchResponse response = esSearchRequest.getResponse(this.esConnection);
        for (SearchHit hit : response.getHits()) {
            bulkRequest.add(new ESConnection.ESDeleteRequestImpl(indexName, hit.getId()));
        }
        return bulkRequest.bulk();
    }

    @Override
    public ESSearchResponse searchAfterId(ESMapping mapping, Object[] searchAfter, Integer limit) {
        return searchAfter(mapping, new String[]{mapping.get_id()}, null, searchAfter, limit);
    }

    @Override
    public ESSearchResponse searchAfter(ESMapping mapping, String[] includes, String[] excludes, Object[] searchAfter, Integer limit) {
        ESConnection.ESSearchRequest esSearchRequest = new ESConnection.ESSearchRequest(mapping.get_index());
        if (searchAfter != null) {
            esSearchRequest.searchAfter(searchAfter);
        }
        esSearchRequest.fetchSource(includes, excludes);
        esSearchRequest.sort(mapping.get_id(), "ASC");
        if (limit != null) {
            esSearchRequest.size(limit);
        }
        SearchResponse response = esSearchRequest.getResponse(this.esConnection);
        ESSearchResponse searchResponse = new ESSearchResponse();
        for (SearchHit hit : response.getHits()) {
            searchResponse.getHitList().add(new Hit(hit.getId(), hit.getSourceAsMap(), hit.getSortValues()));
        }
        return searchResponse;
    }

    @Override
    public Set<String> searchByIds(ESMapping mapping, List<?> ids) {
        ESConnection.ESSearchRequest esSearchRequest = new ESConnection.ESSearchRequest(mapping.get_index());
        esSearchRequest.setQuery(QueryBuilders.idsQuery().addIds(ids.stream().map(String::valueOf).toArray(String[]::new)));
        esSearchRequest.fetchSource("");
        SearchResponse response = esSearchRequest.getResponse(this.esConnection);
        Set<String> result = new LinkedHashSet<>();
        for (SearchHit hit : response.getHits()) {
            result.add(hit.getId());
        }
        return result;
    }

    /**
     * 通过主键删除数据
     *
     * @param mapping 配置对象
     * @param pkVal   主键值
     */
    @Override
    public void delete(ESMapping mapping, Object pkVal, BulkRequestList bulkRequestList) {
        if (pkVal == null || "".equals(pkVal)) {
            return;
        }
        ESConnection.ESDeleteRequestImpl esDeleteRequest = new ESConnection.ESDeleteRequestImpl(mapping.get_index(),
                pkVal.toString());
        addRequest(esDeleteRequest, bulkRequestList);
    }

    /**
     * 提交批次
     *
     * @return ESBulkResponse
     */
    @Override
    public ESBulkRequest.ESBulkResponse commit() {
        if (esBulkRequest.isEmpty()) {
            return ESConnection.EMPTY_RESPONSE;
        }
        long timestamp = System.currentTimeMillis();
        ESBulkRequest.ESBulkResponse response = esBulkRequest.bulk();
        if (!response.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("commit size={}, {}/ms, bytes={}kb, history={}mb, {}", response.size(), System.currentTimeMillis() - timestamp,
                        Math.round((double) response.requestEstimatedSizeInBytes() / 1024),
                        Math.round((double) response.requestTotalEstimatedSizeInBytes() / 1024 / 1024),
                        Arrays.toString(response.requestBytesToString())
                );
            }
            response.processFailBulkResponse(" sync commit error. host: " + Arrays.toString(esConnection.getElasticsearchUri()) + ", detail:");
        }
        return response;
    }

    @Override
    public CompletableFuture<ESBulkRequest.EsRefreshResponse> refresh(Collection<String> indices) {
        return esConnection.refreshAsync(indices.toArray(new String[indices.size()]));
    }

    @Override
    public int bulk(BulkRequestList requests) {
        BulkRequestListImpl bulkRequests = (BulkRequestListImpl) requests;
        List<ESBulkRequest.ESRequest> drainTo = bulkRequests.drainTo();
        if (drainTo.isEmpty()) {
            return 0;
        } else {
            esBulkRequest.add(drainTo, bulkRequests.priorityEnum);
            return drainTo.size();
        }
    }

    private void append4Update(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList) {
        if (pkVal == null || "".equals(pkVal)) {
            return;
        }
        if (esFieldData == null || esFieldData.isEmpty()) {
            return;
        }
        if (pkVal instanceof Collection) {
            int size = ((Collection<?>) pkVal).size();
            if (size == 0) {
                return;
            }
            List<String[]> p;
            int updateByQueryChunkSize = esConnection.getUpdateByQueryChunkSize();
            if (size > updateByQueryChunkSize) {
                List<List<Object>> partition = Lists.partition(new ArrayList<>((Collection<Object>) pkVal), updateByQueryChunkSize);
                p = new ArrayList<>(partition.size());
                for (List<Object> objects : partition) {
                    String[] pkArr = new String[objects.size()];
                    int i = 0;
                    for (Object pk : objects) {
                        pkArr[i++] = String.valueOf(pk);
                    }
                    p.add(pkArr);
                }
            } else {
                String[] pkArr = new String[size];
                int i = 0;
                for (Object pk : (Collection) pkVal) {
                    pkArr[i++] = String.valueOf(pk);
                }
                p = Collections.singletonList(pkArr);
            }
            for (String[] pkArr : p) {
                for (Map.Entry<String, Object> entry : esFieldData.entrySet()) {
                    String fieldName = entry.getKey();
                    if (mapping.isUpdateByQuerySkipIndexUpdatedTime()
                            && mapping.isSetIndexUpdatedTime()
                            && esFieldData.size() > 1
                            && fieldName.equals(mapping.getIndexUpdatedTime())) {
                        continue;
                    }
                    addRequest(ESConnection.ESUpdateByQueryRequestImpl.byIds(mapping.get_index(), pkArr, fieldName, entry.getValue()), bulkRequestList);
                }
            }
        } else {
            String pkToString = pkVal.toString();
            String index = mapping.get_index();
            ESConnection.ESUpdateRequestImpl esUpdateRequest = new ESConnection.ESUpdateRequestImpl(index,
                    pkToString, esFieldData, mapping.isUpsert(), mapping.getRetryOnConflict());
            trimRemoveIndexUpdateTime(mapping, bulkRequestList, esFieldData, index, pkToString);
            addRequest(esUpdateRequest, bulkRequestList);
        }
    }

    private Object getIdValFromRS(ESMapping mapping, ResultSet resultSet) throws SQLException {
        FieldItem idField = mapping.getSchemaItem().getIdField();
        String fieldName = idField.getFieldName();
        String columnName = idField.getColumnName();
        int index;
        try {
            index = resultSet.findColumn(fieldName);
            if (index == -1) {
                index = resultSet.findColumn(columnName);
            }
        } catch (SQLException e) {
            index = resultSet.findColumn(columnName);
        }
        Object value = resultSet.getObject(index);
        return getValFromValue(mapping, value, fieldName, null, Collections.singletonMap(fieldName, value));
    }

    @Override
    public Object getValFromRS(ESMapping mapping, Map<String, Object> row, String fieldName,
                               String columnName, Map<String, Object> data) {
        return getValFromValue(mapping, row.get(fieldName), fieldName, null, row);
    }

    @Override
    public Object getESDataFromRS(ESMapping mapping, Map<String, Object> row,
                                  Map<String, Object> esFieldData, Map<String, Object> data) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        Object resultIdVal = getIdValFromRS(mapping, row);
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            String fieldName = fieldItem.getFieldName();

            Object value = getValFromValue(mapping, row.get(fieldName), fieldName, null, row);

            if (!mapping.isWriteNull() && value == null) {
                continue;
            }
            esFieldData.put(fieldName, value);
        }
        return resultIdVal;
    }

    @Override
    public Object getIdValFromRS(ESMapping mapping, Map<String, Object> row) {
        FieldItem idField = mapping.getSchemaItem().getIdField();
        Object id = row.get(idField.getColumnName());
        if (id == null) {
            id = row.get(idField.getFieldName());
        }
        return id;
    }

    @Override
    public Object getESDataFromRS(ESMapping mapping,
                                  Map<String, Object> row, Map<String, Object> dmlOld,
                                  Map<String, Object> esFieldData,
                                  Map<String, Object> data) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        Object resultIdVal = getIdValFromRS(mapping, row);
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            String fieldName = fieldItem.getFieldName();
            String columnName = fieldItem.getColumnName();

            if (fieldItem.containsColumnName(dmlOld.keySet())) {
                Object newValue = fieldItem.getValue(data);
                Object oldValue = fieldItem.getValue(dmlOld);
                if (!mapping.isWriteNull() && newValue == null && oldValue == null) {
                    continue;
                }
                esFieldData.put(fieldName,
                        getValFromRS(mapping, row, fieldName,
                                columnName, data));
            }
        }

        return resultIdVal;
    }

    @Override
    public Object getValFromData(ESMapping mapping, Map<String, Object> dmlData,
                                 String fieldName, String columnName) {
        return getValFromValue(mapping, dmlData.get(columnName), fieldName, null, dmlData);
    }

    @Override
    public void convertValueType(ESMapping esMapping, String parentFieldName,
                                 Map<String, Object> theConvertMap) {
        for (Map.Entry<String, Object> entry : theConvertMap.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();
            Object newValue = getValFromValue(esMapping, fieldValue, fieldName, parentFieldName, theConvertMap);
            entry.setValue(newValue);
        }
    }

    private Object getValFromValue(ESMapping mapping, Object value, String fieldName, String parentFieldName, Map<String, Object> row) {
        // 如果是对象类型
        ESSyncConfig.ObjectField objectField = mapping.getObjectField(parentFieldName, fieldName);
        if (objectField != null) {
            return objectField.parse(value, mapping, row);
        } else {
            return ESSyncUtil.typeConvert(value, fieldName, getEsType(mapping), parentFieldName);
        }
    }

    /**
     * 将dml的data转换为es的data
     *
     * @param mapping     配置mapping
     * @param dmlData     dml data
     * @param esFieldData es data
     * @return 返回 id 值
     */
    @Override
    public Object getESDataFromDmlData(ESMapping mapping, Map<String, Object> dmlData,
                                       Map<String, Object> esFieldData) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        Object resultIdVal = getIdValFromRS(mapping, dmlData);
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            if (fieldItem.getColumnItems().isEmpty()) {
                continue;
            }
            String columnName = fieldItem.getColumnName();
            String fieldName = fieldItem.getFieldName();

            Object value = getValFromData(mapping, dmlData, fieldName, columnName);

            if (!mapping.isWriteNull() && value == null) {
                continue;
            }
            esFieldData.put(fieldName, value);
        }

        return resultIdVal;
    }

    /**
     * 将dml的data, old转换为es的data
     *
     * @param mapping     配置mapping
     * @param dmlData     dml data
     * @param esFieldData es data
     * @param tableName   tableName
     * @return 返回 id 值
     */
    @Override
    public Object getESDataFromDmlData(ESMapping mapping, Map<String, Object> dmlData, Map<String, Object> dmlOld,
                                       Map<String, Object> esFieldData, String tableName) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        List<String> aliases = schemaItem.getTableItemAliases(tableName);

        Object resultIdVal = getIdValFromRS(mapping, dmlData);
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            if (fieldItem.getColumnItems().isEmpty()) {
                continue;
            }
            String columnName = fieldItem.getColumnName();
            String fieldName = fieldItem.getFieldName();


            /*
             * 修复canal的bug.
             * 针对于 select a.name AS aName, b.name AS bName, c.name AS cName from a left join b left join c 的情况.
             * canal会把修改a表,canal会把 b,c表查询的name字段都改了.
             *
             * 修复方式, 只修改本表的字段. aliases.contains(fieldItem.getOwner())
             *
             *  case : 修改职位名称, 项目名称与公司名称与项目BU名称都变成职位名称了.
             * 王子豪 2019年6月4日 17:39:31
             */
            if (fieldItem.containsOwner(aliases)
                    && fieldItem.containsColumnName(dmlOld.keySet())) {
                Object newValue = fieldItem.getValue(dmlData);
                Object oldValue = fieldItem.getValue(dmlOld);
                if (!mapping.isWriteNull() && newValue == null && oldValue == null) {
                    continue;
                }
                esFieldData.put(fieldName,
                        getValFromData(mapping, dmlData, fieldName, columnName));
            }
        }

        return resultIdVal;
    }

    @Override
    public Object convertFlatValueTypeCopyMap(List<Map<String, Object>> rowList,
                                              ESMapping esMapping,
                                              ESSyncConfig.ObjectField objectField,
                                              String parentFieldName) {
        if (rowList != null && !rowList.isEmpty()) {
            Map<String, Object> map = rowList.get(0);
            Object value0 = ESSyncUtil.value0(map);
            return ESSyncUtil.typeConvert(value0, objectField.getFieldName(), getEsType(esMapping), parentFieldName);
        }
        return null;
    }

    @Override
    public List<Object> convertFlatValueTypeCopyList(List<Map<String, Object>> rowList,
                                                     ESMapping esMapping,
                                                     ESSyncConfig.ObjectField objectField,
                                                     String parentFieldName) {
        if (rowList != null && !rowList.isEmpty()) {
            List<Object> list = new ArrayList<>(rowList.size());
            for (Map<String, Object> row : rowList) {
                Object value0 = ESSyncUtil.value0(row);
                Object cast = ESSyncUtil.typeConvert(value0, objectField.getFieldName(), getEsType(esMapping), parentFieldName);
                list.add(cast);
            }
            return list;
        }
        return Collections.emptyList();
    }

    private Map<String, Object> copyAndConvertType(Map<String, Object> mysqlData, ESMapping mapping) {
        return ESSyncUtil.copyAndConvertType(mysqlData, getEsType(mapping));
    }

    private ESFieldTypesCache getEsType(ESMapping mapping) {
        String index = mapping.get_index();
        long timeout = mapping.getMappingMetadataTimeout();

        ESFieldTypesCache cache = esFieldTypes.get(index);
        if (cache == null || cache.isTimeout(timeout)) {
            synchronized (this) {
                cache = esFieldTypes.get(index);
                if (cache == null || cache.isTimeout(timeout)) {
                    CompletableFuture<Map<String, Object>> future = esConnection.getMapping(index);
                    if (cache == null) {
                        try {
                            Map<String, Object> mappingMetaData = future.get();
                            if (mappingMetaData != null) {
                                esFieldTypes.put(index, cache = new ESFieldTypesCache(mappingMetaData));
                            } else {
                                throw new IllegalArgumentException("Not found the mapping info of index: " + index);
                            }
                        } catch (Exception e) {
                            Util.sneakyThrows(e);
                        }
                    } else {
                        future.whenComplete((mappingMetaData, throwable) -> {
                            if (mappingMetaData != null) {
                                esFieldTypes.put(index, new ESFieldTypesCache(mappingMetaData));
                            } else if (throwable != null) {
                                logger.warn("esConnection.getMapping error {}", throwable.toString(), throwable);
                            }
                        });
                    }
                }
            }
        }
        return cache;
    }

    @Override
    public void close() {
        commit();
        esConnection.close();
    }

    /**
     * 如果大于批量数则提交批次
     */
    private void addRequest(ESBulkRequest.ESUpdateRequest updateRequest, BulkRequestList bulkRequestList) {
        if (bulkRequestList != null) {
            bulkRequestList.add(updateRequest);
            if (isMaxBatchSize(bulkRequestList.size())) {
                int bulk = bulk(bulkRequestList);
                if (bulk > 0) {
                    commit();
                }
            }
        } else {
            esBulkRequest.add(updateRequest);
            if (isMaxBatchSize(esBulkRequest.numberOfActions())) {
                commit();
            }
        }
    }

    private void addRequest(ESBulkRequest.ESUpdateByQueryRequest indexRequest, BulkRequestList bulkRequestList) {
//        synchronized (esBulkRequest) {
        if (bulkRequestList != null) {
            bulkRequestList.add(indexRequest);
            if (isMaxBatchSize(bulkRequestList.size())) {
                int bulk = bulk(bulkRequestList);
                if (bulk > 0) {
                    commit();
                }
            }
        } else {
            esBulkRequest.add(indexRequest);
            if (isMaxBatchSize(esBulkRequest.numberOfActions())) {
                commit();
            }
        }
//        }
    }

    private void addRequest(ESBulkRequest.ESDeleteRequest deleteRequest, BulkRequestList bulkRequestList) {
//        synchronized (esBulkRequest) {
        if (bulkRequestList != null) {
            bulkRequestList.add(deleteRequest);
            if (isMaxBatchSize(bulkRequestList.size())) {
                int bulk = bulk(bulkRequestList);
                if (bulk > 0) {
                    commit();
                }
            }
        } else {
            esBulkRequest.add(deleteRequest);
            if (isMaxBatchSize(esBulkRequest.numberOfActions())) {
                commit();
            }
        }
//        }
    }

    @Override
    public String toString() {
        return "DefaultESTemplate{" +
                "esConnection=" + esConnection +
                ", esBulkRequest=" + esBulkRequest +
                '}';
    }

    public static class BulkRequestListImpl implements BulkRequestList {
        private final List<ESBulkRequest.ESRequest> requests = new ArrayList<>();
        private final BulkPriorityEnum priorityEnum;
        private final Map<String, Set<String>> indexPkUpdateMap;
        private final Consumer<BulkRequestListImpl> addAfter;
        private int size = 0;

        public BulkRequestListImpl(Consumer<BulkRequestListImpl> addAfter, BulkPriorityEnum priorityEnum, Map<String, Set<String>> indexPkUpdateMap) {
            this.addAfter = addAfter;
            this.priorityEnum = priorityEnum;
            this.indexPkUpdateMap = indexPkUpdateMap;
        }

        @Override
        public void add(ESBulkRequest.ESRequest request) {
            synchronized (requests) {
                requests.add(request);
                if (request instanceof ESConnection.ESUpdateByQueryRequestImpl) {
                    size += ((ESConnection.ESUpdateByQueryRequestImpl) request).size();
                } else {
                    size++;
                }
            }
            if (request instanceof ESConnection.ESUpdateRequestImpl) {
                ESConnection.ESUpdateRequestImpl u = ((ESConnection.ESUpdateRequestImpl) request);
                indexPkUpdateMap.computeIfAbsent(u.getIndex(),
                                e -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
                        .add(u.getId());
            }
            addAfter.accept(this);
        }

        public boolean containsUpdate(String index, String id) {
            Set<String> strings = indexPkUpdateMap.get(index);
            return strings != null && strings.contains(id);
        }

        List<ESBulkRequest.ESRequest> drainTo() {
            synchronized (requests) {
                LinkedList<ESBulkRequest.ESRequest> esRequests = new LinkedList<>(requests);
                size = 0;
                requests.clear();
                if (esRequests.size() > 1) {
                    TrimRequest.trim(esRequests);
                }
                return esRequests;
            }
        }

        @Override
        public boolean isEmpty() {
            return size == 0;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public void commit(ESTemplate esTemplate, CommitListener listener) {
            ESBulkRequest.ESBulkResponse commit = esTemplate.commit();
            if (listener != null && !commit.isEmpty()) {
                listener.done(commit);
            }
            if (!requests.isEmpty()) {
                esTemplate.bulk(this);
                ESBulkRequest.ESBulkResponse commit1 = esTemplate.commit();
                if (listener != null && !commit1.isEmpty()) {
                    listener.done(commit1);
                }
            }
        }

        @Override
        public BulkRequestList fork(BulkRequestList bulkRequestList) {
            Map<String, Set<String>> map = new ConcurrentHashMap<>(indexPkUpdateMap);
            if (bulkRequestList instanceof BulkRequestListImpl) {
                map.putAll(((BulkRequestListImpl) bulkRequestList).indexPkUpdateMap);
            }
            return new BulkRequestListImpl(addAfter, priorityEnum, map);
        }

        @Override
        public BulkRequestList fork(BulkPriorityEnum priorityEnum) {
            return new BulkRequestListImpl(addAfter, priorityEnum, indexPkUpdateMap);
        }

        @Override
        public String toString() {
            return "BulkRequestListImpl{" +
                    "size=" + requests.size() +
                    '}';
        }
    }

    public class BulkRequestListAddAfter implements Consumer<BulkRequestListImpl> {

        @Override
        public void accept(BulkRequestListImpl bulkRequestList) {
            if (isMaxBatchSize(bulkRequestList.size)) {
                int bulk = bulk(bulkRequestList);
                if (bulk > 0) {
                    commit();
                }
            }
        }
    }

}
