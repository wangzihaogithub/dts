package com.github.dts.util;

import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.SchemaItem.FieldItem;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * ES 操作模板
 */
public class DefaultESTemplate implements ESTemplate {
    private static final Logger log = LoggerFactory.getLogger(DefaultESTemplate.class);
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
            log.error("sqlRs has error, sql: {} ,异常信息：{}", sql, e.toString());
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
     * @param mapping   配置对象
     * @param pkVal     主键值
     * @param mysqlData 数据Map
     */
    @Override
    public ESBulkRequest.ESBulkResponse insert(ESMapping mapping, Object pkVal, Map<String, Object> mysqlData, BulkRequestList bulkRequestList) {
        if (ESTemplate.isEmptyPk(pkVal)) {
            return ESConnection.EMPTY_RESPONSE;
        }
        Map<String, Object> temp = new LinkedHashMap<>(mysqlData);
        setterIndexUpdatedTime(mapping, temp);

        Map<String, Object> esMap = EsTypeUtil.mysql2EsType(mapping, temp, getEsType(mapping));//insert
        ESBulkRequest.ESUpdateRequest updateRequest = new ESConnection.ESUpdateRequestImpl(mapping.get_index(),
                pkVal.toString(), esMap, true, mapping.getRetryOnConflict());
        return addRequest(updateRequest, bulkRequestList);
    }

    /**
     * 根据主键更新数据
     *
     * @param mapping         配置对象
     * @param pkVal           主键值
     * @param mysqlValue      数据Map
     * @param bulkRequestList bulkRequestList
     */
    @Override
    public void update(ESMapping mapping, Object pkVal, Map<String, Object> mysqlValue, BulkRequestList bulkRequestList) {
        if (ESTemplate.isEmptyUpdate(pkVal, mysqlValue)) {
            return;
        }
        Map<String, Object> temp = new LinkedHashMap<>();
        setterIndexUpdatedTime(mapping, temp);
        temp.putAll(mysqlValue);

        Map<String, Object> esMap = EsTypeUtil.mysql2EsType(mapping, temp, getEsType(mapping));//update
        append4Update(mapping, pkVal, esMap, bulkRequestList);
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

    @Override
    public void updateByQuery(ESSyncConfig config, Map<String, Object> paramsTmp,
                              Map<String, Object> mysqlValueMap, BulkRequestList bulkRequestList) {
        if (paramsTmp.isEmpty()) {
            return;
        }
        if (mysqlValueMap.isEmpty()) {
            return;
        }
        if (paramsTmp.containsKey(ESSyncConfig.ES_ID_FIELD_NAME)) {
            update(config.getEsMapping(), paramsTmp.get(ESSyncConfig.ES_ID_FIELD_NAME), mysqlValueMap, bulkRequestList);//内部调用
            return;
        }

        ESMapping mapping = config.getEsMapping();
        Map<String, Object> temp = new LinkedHashMap<>(mysqlValueMap);
        setterIndexUpdatedTime(mapping, temp);

        Map<String, Object> esMap = EsTypeUtil.mysql2EsType(mapping, temp, getEsType(mapping));//updateByQuery

        // 查询sql批量更新
        DataSource ds = CanalConfig.DatasourceConfig.getDataSource(config.getDataSourceKey());
        StringBuilder sql = new StringBuilder("SELECT * FROM (" + mapping.getSql() + ") _v WHERE ");
        paramsTmp.forEach((fieldName, value) -> sql.append("_v.").append(fieldName).append("=")
                .append(value instanceof Number ? value : "'" + value + "'")
                .append(" AND "));
        int len = sql.length();
        sql.delete(len - 4, len);
        String sqlString = sql.toString();
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);) {
            stmt.setFetchSize(Integer.MIN_VALUE);
            try (ResultSet rs = stmt.executeQuery(sqlString)) {
                while (rs.next()) {
                    Object idVal = getIdValFromRS(mapping, rs);
                    append4Update(mapping, idVal, new LinkedHashMap<>(esMap), bulkRequestList);
                }
            }
        } catch (Exception e) {
            log.error("sqlRs has error, sql: {} ,异常信息：{}", sqlString, e.toString());
            Util.sneakyThrows(e);
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
        return searchAfter(mapping, new String[]{mapping.get_id()}, null, searchAfter, limit, null);
    }

    private static boolean isEmptyQueryBodyJson(Object queryBodyJson) {
        if (queryBodyJson == null) {
            return true;
        }
        if (queryBodyJson instanceof CharSequence) {
            return Util.isBlank(queryBodyJson.toString());
        }
        if (queryBodyJson instanceof Map) {
            return ((Map<?, ?>) queryBodyJson).isEmpty();
        }
        return false;
    }

    @Override
    public ESSearchResponse searchAfter(ESMapping mapping, String[] includes, String[] excludes, Object[] searchAfter, Integer limit, Object queryBodyJson) {
        Map<String, Object> requestBody = new LinkedHashMap<>();
        if (!isEmptyQueryBodyJson(queryBodyJson)) {
            try {
                Map map;
                if (queryBodyJson instanceof CharSequence) {
                    map = JsonUtil.objectReader().readValue(queryBodyJson.toString(), Map.class);
                } else if (queryBodyJson instanceof Map) {
                    map = (Map) queryBodyJson;
                } else {
                    throw new IllegalArgumentException("queryBodyJson no support class type " + queryBodyJson.getClass());
                }
                requestBody.putAll(map);
            } catch (IOException e) {
                Util.sneakyThrows(e);
            }
        }
        if (searchAfter != null) {
            requestBody.put("search_after", searchAfter);
        }
        if (limit != null) {
            requestBody.put("size", limit);
        }
        Map<String, Object> _source = new HashMap<>(2);
        if (includes != null && includes.length > 0) {
            _source.put("includes", includes);
        }
        if (excludes != null && excludes.length > 0) {
            _source.put("excludes", excludes);
        }
        if (!_source.isEmpty()) {
            requestBody.put("_source", _source);
        }
        requestBody.put("sort", Collections.singletonList(Collections.singletonMap(mapping.get_id(), Collections.singletonMap("order", "ASC"))));
        ESSearchResponse searchResponse = new ESSearchResponse();
        try {
            CompletableFuture<Map<String, Object>> search = esConnection.search(mapping.get_index(), requestBody);
            try {
                Map<String, Object> responseBody = search.get();
                Map<String, Object> hits = (Map) responseBody.get("hits");
                List<Map<String, Object>> rowList = (List) hits.get("hits");
                for (Map<String, Object> row : rowList) {
                    Object id = row.get("_id");
                    Map source = (Map) row.get("_source");
                    List<Object> sort = (List) row.get("sort");
                    searchResponse.getHitList().add(new Hit(Objects.toString(id, null), source, sort.toArray()));
                }
            } catch (InterruptedException | ExecutionException e) {
                Util.sneakyThrows(e);
            }
        } catch (IOException e) {
            // 客户端没请求过去
            Util.sneakyThrows(e);
        }
        return searchResponse;
    }

    @Override
    public Set<String> searchByIds(ESMapping mapping, List<?> ids) {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptySet();
        }
        ESConnection.ESSearchRequest esSearchRequest = new ESConnection.ESSearchRequest(mapping.get_index());
        esSearchRequest.setQuery(QueryBuilders.idsQuery().addIds(ids.stream().map(String::valueOf).toArray(String[]::new)));
        esSearchRequest.size(ids.size());
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
    public ESBulkRequest.ESBulkResponse delete(ESMapping mapping, Object pkVal, BulkRequestList bulkRequestList) {
        if (ESTemplate.isEmptyPk(pkVal)) {
            return ESConnection.EMPTY_RESPONSE;
        }
        ESConnection.ESDeleteRequestImpl esDeleteRequest = new ESConnection.ESDeleteRequestImpl(mapping.get_index(),
                pkVal.toString());
        return addRequest(esDeleteRequest, bulkRequestList);
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
            if (log.isInfoEnabled()) {
                double kb = Math.round((double) response.requestEstimatedSizeInBytes() * 100D / 1024D) / 100D;
                double mb = Math.round((double) response.requestTotalEstimatedSizeInBytes() * 100D / 1024D / 1024D) / 100D;
                log.info("commit size={}, {}/ms, bytes={}kb, history={}mb, {}", response.size(), System.currentTimeMillis() - timestamp,
                        (kb >= 1D || kb == 0D ? String.valueOf(Math.round(kb)) : String.valueOf(kb)),
                        (mb >= 1D || mb == 0D ? String.valueOf(Math.round(mb)) : String.valueOf(mb)),
                        Arrays.toString(response.requestBytesToString())
                );
            }
            response.processFailBulkResponse(" sync commit error. host: " + Arrays.toString(esConnection.getElasticsearchUri()) + ", detail:");
        }
        return response;
    }

    /**
     * aliases至新表
     *
     * @param aliasIndexName 别名
     * @param newIndexName   新索引名称（如果不存在，则自动创建结构）
     * @return 是否成功
     */
    public EsActionResponse indexAliasesTo(String aliasIndexName, String newIndexName) throws IOException {
        // 新索引是否存在
        boolean newIndexExist = esConnection.indexExist(newIndexName);
        EsIndexMetaGetResponse metaGetResponse = esConnection.indexMetaGet(aliasIndexName);
        if (!metaGetResponse.isSuccess()) {
            return metaGetResponse;
        }

        // 新索引不存在自动创建
        EsActionResponse indexCreateResponse = null;
        if (!newIndexExist) {
            indexCreateResponse = esConnection.indexCreate(newIndexName, metaGetResponse.toCreateIndexMeta());
        }
        // 如果创建失败
        if (indexCreateResponse != null && !indexCreateResponse.isSuccess()) {
            return indexCreateResponse;
        }
        String responseAliasesIndexName = Optional.ofNullable(metaGetResponse.getResponseAliasesIndexName()).orElse(aliasIndexName);
        String sourceIndexName = metaGetResponse.getResponseIndexName();
        return esConnection.aliases(responseAliasesIndexName, sourceIndexName, responseAliasesIndexName, newIndexName);
    }

    /**
     * reindex
     *
     * @param aliasIndexName         别名
     * @param newIndexName           新索引名称（如果不存在，则自动创建结构）
     * @param afterAliasRemoveAndAdd 是否需要重新关联，reindex后，绑定至别名
     * @return 是否成功
     */
    public EsTaskCompletableFuture<EsActionResponse> reindex(String aliasIndexName, String newIndexName, boolean afterAliasRemoveAndAdd) {
        EsTaskCompletableFuture<EsActionResponse> future = new EsTaskCompletableFuture<>();
        try {
            // 新索引是否存在
            boolean newIndexExist = esConnection.indexExist(newIndexName);
            EsIndexMetaGetResponse metaGetResponse = esConnection.indexMetaGet(aliasIndexName);
            if (!metaGetResponse.isSuccess()) {
                future.complete(metaGetResponse);
                return future;
            }

            // 新索引不存在自动创建
            EsActionResponse indexCreateResponse = null;
            if (!newIndexExist) {
                indexCreateResponse = esConnection.indexCreate(newIndexName, metaGetResponse.toCreateIndexMeta());
            }
            // 如果创建失败
            if (indexCreateResponse != null && !indexCreateResponse.isSuccess()) {
                future.complete(indexCreateResponse);
                return future;
            }
            String responseAliasesIndexName = Optional.ofNullable(metaGetResponse.getResponseAliasesIndexName()).orElse(aliasIndexName);
            String sourceIndexName = metaGetResponse.getResponseIndexName();
            // 拷贝数据
            EsTask reindex = esConnection.reindex(sourceIndexName, newIndexName);
            future.setTaskId(reindex.getId());
            log.info("reindex taskId = {},  sourceIndex = {}, destIndex = {}", reindex.getId(), sourceIndexName, newIndexName);
            reindex.thenAccept(esTaskResponse -> {
                        // 如果需要重新关联
                        if (afterAliasRemoveAndAdd) {
                            try {
                                // 删除旧引用，关联新索引
                                EsActionResponse aliases = esConnection.aliases(responseAliasesIndexName, sourceIndexName, responseAliasesIndexName, newIndexName);
                                future.complete(aliases);
                            } catch (IOException e) {
                                future.completeExceptionally(e);
                            }
                        } else {
                            // 不需要重新关联
                            future.complete(esTaskResponse);
                        }
                    })
                    .exceptionally(throwable -> {
                        future.completeExceptionally(throwable);
                        return null;
                    });
        } catch (Throwable e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<ESBulkRequest.EsRefreshResponse> refresh(Collection<String> indices) {
        return esConnection.refreshAsync(indices.toArray(new String[indices.size()]));
    }

    public EsTask deleteByQuery(String indexName, Map<String, Object> httpBody, Map<String, Object> httpQuery) throws IOException {
        return esConnection.deleteByQuery(indexName, httpBody, httpQuery);
    }

    public EsTask updateByQuery(String indexName, Map<String, Object> httpBody, Map<String, Object> httpQuery) throws IOException {
        return esConnection.updateByQuery(indexName, httpBody, httpQuery);
    }

    public <T> T searchMaxValue(String indexName, String fieldName, Class<T> type) {
        ESConnection.ESSearchRequest request = new ESConnection.ESSearchRequest(indexName);
        request.size(0);
        request.aggregation(AggregationBuilders.max(fieldName).field(fieldName));
        SearchResponse response;
        try {
            response = request.getResponse(esConnection);
        } catch (ElasticsearchStatusException e) {
            if (e.status() == RestStatus.BAD_REQUEST) {
                ESConnection.ESSearchRequest requestRetry = new ESConnection.ESSearchRequest(indexName);
                requestRetry.size(0);
                requestRetry.aggregation(AggregationBuilders.max(fieldName).script(new Script(ScriptType.INLINE, "painless",
                        "Double.parseDouble(doc['" + fieldName + "'].value)", // 转为 double
                        Collections.emptyMap())));
                response = requestRetry.getResponse(esConnection);
            } else {
                throw e;
            }
        }
        NumericMetricsAggregation.SingleValue singleValue = response.getAggregations().get(fieldName);
        if (singleValue == null) {
            return null;
        }
        double value;
        try {
            value = singleValue.value();
        } catch (NullPointerException e) {
            // 没有记录
            return null;
        }
        return TypeUtil.cast(value, type);
    }

    public <T> T searchMinValue(String indexName, String fieldName, Class<T> type) {
        ESConnection.ESSearchRequest request = new ESConnection.ESSearchRequest(indexName);
        request.size(0);
        request.aggregation(AggregationBuilders.min(fieldName).field(fieldName));
        SearchResponse response;
        try {
            response = request.getResponse(esConnection);
        } catch (ElasticsearchStatusException e) {
            if (e.status() == RestStatus.BAD_REQUEST) {
                ESConnection.ESSearchRequest requestRetry = new ESConnection.ESSearchRequest(indexName);
                requestRetry.size(0);
                requestRetry.aggregation(AggregationBuilders.min(fieldName).script(new Script(ScriptType.INLINE, "painless",
                        "Double.parseDouble(doc['" + fieldName + "'].value)", // 转为 double
                        Collections.emptyMap())));
                response = requestRetry.getResponse(esConnection);
            } else {
                throw e;
            }
        }
        NumericMetricsAggregation.SingleValue singleValue = response.getAggregations().get(fieldName);
        if (singleValue == null) {
            return null;
        }
        double value;
        try {
            value = singleValue.value();
        } catch (NullPointerException e) {
            // 没有记录
            return null;
        }
        return TypeUtil.cast(value, type);
    }

    public EsTask forcemerge(String indexName, Integer maxNumSegments, Boolean onlyExpungeDeletes, Boolean flush) throws IOException {
        return esConnection.forcemerge(indexName, maxNumSegments, onlyExpungeDeletes, flush);
    }

    public ESConnection getConnection() {
        return esConnection;
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
        return resultSet.getObject(index);
    }

    private ESFieldTypesCache getEsType(ESMapping mapping) {
        String index = mapping.get_index();
        long timeout = mapping.getMappingMetadataTimeout();
        String esUri = String.join(",", esConnection.getElasticsearchUri());
        String cacheKey = esUri + "_" + index;
        ESFieldTypesCache cache = esFieldTypes.get(cacheKey);
        if (cache == null || cache.isTimeout(timeout)) {
            synchronized (this) {
                cache = esFieldTypes.get(cacheKey);
                if (cache == null || cache.isTimeout(timeout)) {
                    CompletableFuture<Map<String, Object>> future = esConnection.getMapping(index);
                    if (cache == null) {
                        try {
                            Map<String, Object> mappingMetaData = future.get();
                            if (mappingMetaData != null) {
                                esFieldTypes.put(cacheKey, cache = new ESFieldTypesCache(mappingMetaData));
                            } else {
                                throw new IllegalArgumentException("Not found the mapping info of index: " + index + ", esUri: " + esUri);
                            }
                        } catch (Exception e) {
                            Util.sneakyThrows(e);
                        }
                    } else {
                        future.whenComplete((mappingMetaData, throwable) -> {
                            if (mappingMetaData != null) {
                                esFieldTypes.put(cacheKey, new ESFieldTypesCache(mappingMetaData));
                            } else if (throwable != null) {
                                log.warn("esConnection.getMapping error {}", throwable.toString(), throwable);
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
        commit();//close
        esConnection.close();
    }

    /**
     * 如果大于批量数则提交批次
     */
    private ESBulkRequest.ESBulkResponse addRequest(ESBulkRequest.ESUpdateRequest updateRequest, BulkRequestList bulkRequestList) {
        if (bulkRequestList != null) {
            bulkRequestList.add(updateRequest);
            if (isMaxBatchSize(bulkRequestList.size())) {
                int bulk = bulk(bulkRequestList);
                if (bulk > 0) {
                    return commit();
                }
            }
        } else {
            esBulkRequest.add(updateRequest);
            if (isMaxBatchSize(esBulkRequest.numberOfActions())) {
                return commit();
            }
        }
        return ESConnection.EMPTY_RESPONSE;
    }

    private ESBulkRequest.ESBulkResponse addRequest(ESBulkRequest.ESUpdateByQueryRequest indexRequest, BulkRequestList bulkRequestList) {
//        synchronized (esBulkRequest) {
        if (bulkRequestList != null) {
            bulkRequestList.add(indexRequest);
            if (isMaxBatchSize(bulkRequestList.size())) {
                int bulk = bulk(bulkRequestList);
                if (bulk > 0) {
                    return commit();
                }
            }
        } else {
            esBulkRequest.add(indexRequest);
            if (isMaxBatchSize(esBulkRequest.numberOfActions())) {
                return commit();
            }
        }
        return ESConnection.EMPTY_RESPONSE;
//        }
    }

    private ESBulkRequest.ESBulkResponse addRequest(ESBulkRequest.ESDeleteRequest deleteRequest, BulkRequestList bulkRequestList) {
//        synchronized (esBulkRequest) {
        if (bulkRequestList != null) {
            bulkRequestList.add(deleteRequest);
            if (isMaxBatchSize(bulkRequestList.size())) {
                int bulk = bulk(bulkRequestList);
                if (bulk > 0) {
                    return commit();
                }
            }
        } else {
            esBulkRequest.add(deleteRequest);
            if (isMaxBatchSize(esBulkRequest.numberOfActions())) {
                return commit();
            }
        }
        return ESConnection.EMPTY_RESPONSE;
//        }
    }

    @Override
    public String toString() {
        return "DefaultESTemplate{" +
                "connection=" + esConnection +
                ", request=" + esBulkRequest +
                '}';
    }

    public static class BulkRequestListImpl implements BulkRequestList {
        private final List<ESBulkRequest.ESRequest> requests = new ArrayList<>();
        private final BulkPriorityEnum priorityEnum;
        private final Map<String, Set<String>> indexPkUpdateMap;
        private final Function<BulkRequestListImpl, ESBulkRequest.ESBulkResponse> addAfter;
        private int size = 0;
        private int commitRequests = 0;

        public BulkRequestListImpl(Function<BulkRequestListImpl, ESBulkRequest.ESBulkResponse> addAfter, BulkPriorityEnum priorityEnum, Map<String, Set<String>> indexPkUpdateMap) {
            this.addAfter = addAfter;
            this.priorityEnum = priorityEnum;
            this.indexPkUpdateMap = indexPkUpdateMap;
        }

        @Override
        public ESBulkRequest.ESBulkResponse add(ESBulkRequest.ESRequest request) {
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
            return addAfter.apply(this);
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
        public int commitRequests() {
            return commitRequests;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public ESBulkRequest.ESBulkResponse commit(ESTemplate esTemplate, CommitListener listener) {
            ESBulkRequest.ESBulkResponse commit = esTemplate.commit();
            if (listener != null && !commit.isEmpty()) {
                listener.done(commit);
                commitRequests += commit.size();
            }
            ESBulkRequest.ESBulkResponse commit1 = null;
            if (!requests.isEmpty()) {
                esTemplate.bulk(this);
                commit1 = esTemplate.commit();
                if (listener != null && !commit1.isEmpty()) {
                    listener.done(commit1);
                    commitRequests += commit1.size();
                }
            }
            return ESConnection.ESBulkResponseImpl.merge(commit, commit1);
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

    public class BulkRequestListAddAfter implements Function<BulkRequestListImpl, ESBulkRequest.ESBulkResponse> {

        @Override
        public ESBulkRequest.ESBulkResponse apply(BulkRequestListImpl bulkRequestList) {
            if (isMaxBatchSize(bulkRequestList.size)) {
                int bulk = bulk(bulkRequestList);
                if (bulk > 0) {
                    return commit();
                }
            }
            return ESConnection.EMPTY_RESPONSE;
        }

    }

}
