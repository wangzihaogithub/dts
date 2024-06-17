package com.github.dts.util;

import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.SchemaItem.FieldItem;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ES 操作模板
 */
public class ES7xTemplate implements ESTemplate {
    private static final Logger logger = LoggerFactory.getLogger(ES7xTemplate.class);
    private static final ConcurrentMap<String, EsFieldTypesCache> esFieldTypes = new ConcurrentHashMap<>();
    private final ES7xConnection esConnection;
    private final ES7xConnection.ES7xBulkRequest esBulkRequest;

    public ES7xTemplate(ES7xConnection esConnection) {
        this.esConnection = esConnection;
        this.esBulkRequest = new ES7xConnection.ES7xBulkRequest(esConnection);
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
        setterIndexUpdatedTime(mapping, esFieldData);

        esFieldData = ESSyncUtil.convertType(esFieldData);
        if (mapping.get_id() != null) {
            String parentVal = (String) esFieldData.remove("$parent_routing");
            ESBulkRequest.ESUpdateRequest updateRequest = new ES7xConnection.ES7xUpdateRequest(mapping.get_index(),
                    pkVal.toString()).setDoc(esFieldData).setDocAsUpsert(true);
            if (Util.isNotEmpty(parentVal)) {
                updateRequest.setRouting(parentVal);
            }
            addRequest(updateRequest, bulkRequestList);
        } else {
            ES7xConnection.ESSearchRequest esSearchRequest = new ES7xConnection.ESSearchRequest(mapping.get_index())
                    .setQuery(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                    .size(10000);
            SearchResponse response = esSearchRequest.getResponse(this.esConnection);

            for (SearchHit hit : response.getHits()) {
                ESBulkRequest.ESUpdateRequest esUpdateRequest = new ES7xConnection.ES7xUpdateRequest(mapping.get_index(),
                        hit.getId()).setDoc(esFieldData);
                addRequest(esUpdateRequest, bulkRequestList);
            }
        }
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
        if (pkVal == null || "".equals(pkVal)) {
            return;
        }
        if (esFieldData.isEmpty()) {
            return;
        }
        Map<String, Object> esFieldDataTmp = new LinkedHashMap<>(esFieldData.size());
        esFieldData.forEach((k, v) -> esFieldDataTmp.put(Util.cleanColumn(k), v));

        setterIndexUpdatedTime(mapping, esFieldDataTmp);

        Map<String, Object> esFieldDataConvert = ESSyncUtil.convertType(esFieldDataTmp);
        append4Update(mapping, pkVal, esFieldDataConvert, bulkRequestList);
    }

    private void setterIndexUpdatedTime(ESMapping mapping, Map<String, Object> esFieldData) {
        String indexUpdatedTime = mapping.getIndexUpdatedTime();
        if (indexUpdatedTime != null && !indexUpdatedTime.isEmpty()) {
            esFieldData.put(indexUpdatedTime, new Date());
        }
    }

    /**
     * update by query
     * 2019年5月27日 16:35:37 王子豪
     *
     * @param mapping          配置对象
     * @param esFieldDataWhere ES更新条件
     * @param esFieldData      数据Map
     */
    @Override
    public void updateByQuery(ESMapping
                                      mapping, Map<String, Object> esFieldDataWhere, Map<String, Object> esFieldData, BulkRequestList bulkRequestList) {
        if (esFieldDataWhere.isEmpty()) {
            return;
        }
        if (esFieldData.isEmpty()) {
            return;
        }
        esFieldData = ESSyncUtil.convertType(esFieldData);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        esFieldDataWhere.forEach((fieldName, value) -> queryBuilder.must(QueryBuilders.termsQuery(fieldName, value)));
        Map<String, Object> esFieldDataTmp = new LinkedHashMap<>(esFieldData.size());
        esFieldData.forEach((k, v) -> esFieldDataTmp.put(Util.cleanColumn(k), v));

        setterIndexUpdatedTime(mapping, esFieldData);

        ES7xConnection.ESSearchRequest esSearchRequest = new ES7xConnection.ESSearchRequest(mapping.get_index())
                .setQuery(queryBuilder)
                .size(10000);
        SearchResponse response = esSearchRequest.getResponse(this.esConnection);
        for (SearchHit hit : response.getHits()) {
            append4Update(mapping, hit.getId(), esFieldDataTmp, bulkRequestList);
        }
    }

    @Override
    public BulkRequestList newBulkRequestList() {
        return new BulkRequestListImpl();
    }

    /**
     * 脚本更新
     *
     * @param mapping  索引
     * @param idOrCode 脚本
     * @param isUpsert 如果数据不存在是否新增
     * @param pkValue  主键
     */
    @Override
    public void updateByScript(ESMapping mapping, String idOrCode, boolean isUpsert, Object pkValue,
                               int scriptTypeId, String lang, Map<String, Object> params, BulkRequestList bulkRequestList) {
        if (idOrCode == null) {
            return;
        }
        if (lang == null) {
            lang = "painless";
        }
        ScriptType scriptType;
        if (scriptTypeId == ScriptType.INLINE.getId()) {
            scriptType = ScriptType.INLINE;
        } else {
            scriptType = ScriptType.STORED;
        }
        if (pkValue == null || "".equals(pkValue)) {
            return;
        }
        params = ESSyncUtil.convertType(params);
        Script script = new Script(scriptType, lang, idOrCode, params);
        ESBulkRequest.ESUpdateRequest esUpdateRequest = new ES7xConnection.ES7xUpdateRequest(mapping.get_index(),
                pkValue.toString()).setScript(script);
        addRequest(esUpdateRequest, bulkRequestList);
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
        Map<String, Object> esFieldDataTmp = ESSyncUtil.convertType(esFieldData);
        ESMapping mapping = config.getEsMapping();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        paramsTmp.forEach((fieldName, value) -> queryBuilder.must(QueryBuilders.termsQuery(fieldName, value)));

        // 查询sql批量更新
        DataSource ds = CanalConfig.DatasourceConfig.getDataSource(config.getDataSourceKey());
        StringBuilder sql = new StringBuilder("SELECT * FROM (" + mapping.getSql() + ") _v WHERE ");
        paramsTmp.forEach(
                (fieldName, value) -> sql.append("_v.").append(fieldName).append("=").append(value).append(" AND "));
        int len = sql.length();
        sql.delete(len - 4, len);
        Integer syncCount = (Integer) Util.sqlRS(ds, sql.toString(), rs -> {
            int count = 0;
            try {
                while (rs.next()) {
                    Object idVal = getIdValFromRS(mapping, rs);
                    append4Update(mapping, idVal, esFieldDataTmp, bulkRequestList);
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

    @Override
    public ESBulkRequest.ESBulkResponse deleteByIdRange(ESMapping mapping, Integer minId, Integer maxId) {
        if (minId == null || maxId == null) {
            return null;
        }
        String idColName = mapping.get_id();
        if (idColName == null) {
            return null;
        }
        String indexName = mapping.get_index();

        ES7xConnection.ESSearchRequest esSearchRequest = new ES7xConnection.ESSearchRequest(indexName)
                .setQuery(QueryBuilders.rangeQuery(idColName).lte(maxId).gte(minId))
                .fetchSource(idColName)
                .size(10000);
        SearchResponse response = esSearchRequest.getResponse(this.esConnection);

        ES7xConnection.ES7xBulkRequest bulkRequest = new ES7xConnection.ES7xBulkRequest(esConnection);
        for (SearchHit hit : response.getHits()) {
            bulkRequest.add(new ES7xConnection.ES7xDeleteRequest(indexName, hit.getId()));
        }
        return bulkRequest.bulk();
    }

    /**
     * 通过主键删除数据
     *
     * @param mapping     配置对象
     * @param pkVal       主键值
     * @param esFieldData 数据Map
     */
    @Override
    public void delete(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData, BulkRequestList bulkRequestList) {
        if (pkVal == null || "".equals(pkVal)) {
            return;
        }
        if (mapping.get_id() != null) {
            ES7xConnection.ES7xDeleteRequest esDeleteRequest = new ES7xConnection.ES7xDeleteRequest(mapping.get_index(),
                    pkVal.toString());
            addRequest(esDeleteRequest, bulkRequestList);
        } else {
            if (esFieldData == null || esFieldData.isEmpty()) {
                return;
            }
            esFieldData = ESSyncUtil.convertType(esFieldData);
            ES7xConnection.ESSearchRequest esSearchRequest = new ES7xConnection.ESSearchRequest(mapping.get_index())
                    .setQuery(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                    .size(10000);
            SearchResponse response = esSearchRequest.getResponse(this.esConnection);
            for (SearchHit hit : response.getHits()) {
                ES7xConnection.ES7xUpdateRequest esUpdateRequest = new ES7xConnection.ES7xUpdateRequest(mapping.get_index(),
                        hit.getId()).setDoc(esFieldData);
                addRequest(esUpdateRequest, bulkRequestList);
            }
        }
    }

    /**
     * 提交批次
     */
    @Override
    public void commit() {
        if (esBulkRequest.isEmpty()) {
            return;
        }
        long timestamp = System.currentTimeMillis();
        ESBulkRequest.ESBulkResponse response = esBulkRequest.bulk();
        if (!response.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("commit size={}, {}/ms ", response.size(), System.currentTimeMillis() - timestamp);
            }
            response.processFailBulkResponse("ES7 sync commit error. ");
        }
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
            esBulkRequest.add(drainTo);
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
        if (mapping.get_id() != null) {
            String parentVal = (String) esFieldData.remove("$parent_routing");
            ES7xConnection.ES7xUpdateRequest esUpdateRequest = new ES7xConnection.ES7xUpdateRequest(mapping.get_index(),
                    pkVal.toString()).setDoc(esFieldData).setDocAsUpsert(mapping.isUpsert());
            if (Util.isNotEmpty(parentVal)) {
                esUpdateRequest.setRouting(parentVal);
            }
            addRequest(esUpdateRequest, bulkRequestList);
        } else {
            ES7xConnection.ESSearchRequest esSearchRequest = new ES7xConnection.ESSearchRequest(mapping.get_index())
                    .setQuery(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                    .size(10000);
            SearchResponse response = esSearchRequest.getResponse(this.esConnection);
            for (SearchHit hit : response.getHits()) {
                ESBulkRequest.ESUpdateRequest esUpdateRequest = new ES7xConnection.ES7xUpdateRequest(mapping.get_index(),
                        hit.getId()).setDoc(esFieldData);
                addRequest(esUpdateRequest, bulkRequestList);
            }
        }
    }

    public int getColIndexFromRS(ResultSet resultSet, String fieldName, String columnName) throws SQLException {
        int index;
        try {
            index = resultSet.findColumn(fieldName);
            if (index == -1) {
                index = resultSet.findColumn(columnName);
            }
        } catch (SQLException e) {
            index = resultSet.findColumn(columnName);
        }
        return index;
    }

    @Override
    public Object getValFromRS(ESMapping mapping, ResultSet resultSet, String fieldName,
                               String columnName, Map<String, Object> data) throws SQLException {
//        fieldName = Util.cleanColumn(fieldName);
//        columnName = Util.cleanColumn(columnName);
        String esType = getEsType(mapping, fieldName);

        int colIndexFromRS = getColIndexFromRS(resultSet, fieldName, columnName);
        Object value = resultSet.getObject(colIndexFromRS);
        if (value instanceof Boolean) {
            if (!"boolean".equals(esType)) {
                value = resultSet.getByte(colIndexFromRS);
            }
        }

        // 如果是对象类型
        if (mapping.getObjFields().containsKey(fieldName)) {
            return ESSyncUtil.convertToEsObj(value, mapping, fieldName, data);
        } else {
            return ESSyncUtil.typeConvert(value, esType);
        }
    }

    @Override
    public Object getESDataFromRS(ESMapping mapping, ResultSet resultSet,
                                  Map<String, Object> esFieldData, Map<String, Object> data) throws SQLException {
        SchemaItem schemaItem = mapping.getSchemaItem();
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            String columnName = fieldItem.getColumnName();
            String fieldName = fieldItem.getFieldName();

            Object value = getValFromRS(mapping, resultSet, fieldName, columnName, data);

            if (fieldItem.equalsField(idFieldName)) {
                resultIdVal = value;
            }

            if (!mapping.getSkips().contains(fieldName)) {
                if (!mapping.isWriteNull() && value == null) {
                    continue;
                }
                esFieldData.put(fieldName, value);
            }
        }

        // 添加父子文档关联信息
        putRelationDataFromRS(mapping, schemaItem, resultSet, esFieldData, data);

        return resultIdVal;
    }

    @Override
    public Object getIdValFromRS(ESMapping mapping, ResultSet resultSet) throws SQLException {
        SchemaItem schemaItem = mapping.getSchemaItem();
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            Object value = getValFromRS(mapping, resultSet, fieldItem.getFieldName(),
                    fieldItem.getColumnName(), null);

            if (fieldItem.equalsField(idFieldName)) {
                resultIdVal = value;
                break;
            }
        }
        return resultIdVal;
    }

    @Override
    public Object getESDataFromRS(ESMapping mapping, ResultSet resultSet, Map<String, Object> dmlOld,
                                  Map<String, Object> esFieldData,
                                  Map<String, Object> data) throws SQLException {
        SchemaItem schemaItem = mapping.getSchemaItem();
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            String fieldName = fieldItem.getFieldName();
            String columnName = fieldItem.getColumnName();

            if (fieldItem.equalsField(idFieldName)) {
                resultIdVal = getValFromRS(mapping, resultSet, fieldName,
                        columnName, data);
            }
            if (fieldItem.containsColumnName(dmlOld.keySet())
                    && !mapping.getSkips().contains(fieldName)) {
                Object newValue = fieldItem.getValue(data);
                Object oldValue = fieldItem.getValue(dmlOld);
                if (!mapping.isWriteNull() && newValue == null && oldValue == null) {
                    continue;
                }
                esFieldData.put(fieldName,
                        getValFromRS(mapping, resultSet, fieldName,
                                columnName, data));
            }
        }

        // 添加父子文档关联信息
        putRelationDataFromRS(mapping, schemaItem, resultSet, esFieldData, data);

        return resultIdVal;
    }

    @Override
    public Object getValFromData(ESMapping mapping, Map<String, Object> dmlData, String fieldName, String
            columnName) {
        String esType = getEsType(mapping, fieldName);
        Object value = dmlData.get(columnName);
        if (value instanceof Byte) {
            if ("boolean".equals(esType)) {
                value = ((Byte) value).intValue() != 0;
            }
        }

        // 如果是对象类型
        if (mapping.getObjFields().containsKey(fieldName)) {
            return ESSyncUtil.convertToEsObj(value, mapping, fieldName, dmlData);
        } else {
            return ESSyncUtil.typeConvert(value, esType);
        }
    }

    @Override
    public Object getValFromValue(ESMapping mapping, Object value, Map<String, Object> dmlData, String fieldName) {
        String esType = getEsType(mapping, fieldName);
        // 如果是对象类型
        if (mapping.getObjFields().containsKey(fieldName)) {
            return ESSyncUtil.convertToEsObj(value, mapping, fieldName, dmlData);
        } else {
            return ESSyncUtil.typeConvert(value, esType);
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
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            if (fieldItem.getColumnItems().isEmpty()) {
                continue;
            }
            String columnName = fieldItem.getColumnName();
            String fieldName = fieldItem.getFieldName();

            Object value = getValFromData(mapping, dmlData, fieldName, columnName);

            if (fieldItem.equalsField(idFieldName)) {
                resultIdVal = value;
            }

            if (!mapping.getSkips().contains(fieldName)) {
                if (!mapping.isWriteNull() && value == null) {
                    continue;
                }
                esFieldData.put(fieldName, value);
            }
        }

        // 添加父子文档关联信息
        putRelationData(mapping, schemaItem, dmlData, esFieldData);
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
    public Object getESDataFromDmlData(ESMapping
                                               mapping, Map<String, Object> dmlData, Map<String, Object> dmlOld,
                                       Map<String, Object> esFieldData, String tableName) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        List<String> aliases = schemaItem.getTableItemAliases(tableName);

        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            if (fieldItem.getColumnItems().isEmpty()) {
                continue;
            }
            String columnName = fieldItem.getColumnName();
            String fieldName = fieldItem.getFieldName();

            if (fieldItem.equalsField(idFieldName)) {
                resultIdVal = getValFromData(mapping, dmlData, fieldName, columnName);
            }

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
                    && fieldItem.containsColumnName(dmlOld.keySet())
                    && !mapping.getSkips().contains(fieldName)) {
                Object newValue = fieldItem.getValue(dmlData);
                Object oldValue = fieldItem.getValue(dmlOld);
                if (!mapping.isWriteNull() && newValue == null && oldValue == null) {
                    continue;
                }
                esFieldData.put(fieldName,
                        getValFromData(mapping, dmlData, fieldName, columnName));
            }
        }

        // 添加父子文档关联信息
        putRelationData(mapping, schemaItem, dmlOld, esFieldData);
        return resultIdVal;
    }

    private void putRelationDataFromRS(ESMapping mapping, SchemaItem schemaItem, ResultSet resultSet,
                                       Map<String, Object> esFieldData,
                                       Map<String, Object> data) {
        // 添加父子文档关联信息
        if (mapping.getRelations().isEmpty()) {
            return;
        }
        mapping.getRelations().forEach((relationField, relationMapping) -> {
            Map<String, Object> relations = new HashMap<>();
            relations.put("name", relationMapping.getName());
            if (StringUtils.isNotEmpty(relationMapping.getParent())) {
                FieldItem parentFieldItem = schemaItem.getSelectFields().get(relationMapping.getParent());
                Object parentVal;
                try {
                    parentVal = getValFromRS(mapping, resultSet, parentFieldItem.getFieldName(), parentFieldItem.getColumnName(),
                            data);
                } catch (SQLException e) {
                    Util.sneakyThrows(e);
                    parentVal = null;
                }
                if (parentVal != null) {
                    relations.put("parent", parentVal.toString());
                    esFieldData.put("$parent_routing", parentVal.toString());

                }
            }
            esFieldData.put(relationField, relations);
        });
    }

    private void putRelationData(ESMapping mapping, SchemaItem schemaItem, Map<String, Object> dmlData,
                                 Map<String, Object> esFieldData) {
        // 添加父子文档关联信息
        if (!mapping.getRelations().isEmpty()) {
            mapping.getRelations().forEach((relationField, relationMapping) -> {
                Map<String, Object> relations = new HashMap<>();
                relations.put("name", relationMapping.getName());
                if (Util.isNotEmpty(relationMapping.getParent())) {
                    FieldItem parentFieldItem = schemaItem.getSelectFields().get(relationMapping.getParent());
                    String columnName = parentFieldItem.getColumnItems().iterator().next().getColumnName();
                    Object parentVal = getValFromData(mapping, dmlData, parentFieldItem.getFieldName(), columnName);
                    if (parentVal != null) {
                        relations.put("parent", parentVal.toString());
                        esFieldData.put("$parent_routing", parentVal.toString());
                    }
                }
                esFieldData.put(relationField, relations);
            });
        }
    }

    /**
     * 获取es mapping中的属性类型
     *
     * @param mapping   mapping配置
     * @param fieldName 属性名
     * @return 类型
     */
    @SuppressWarnings("unchecked")
    private String getEsType(ESMapping mapping, String fieldName) {
        String key = mapping.get_index();
        EsFieldTypesCache cache = esFieldTypes.get(key);

        if (cache == null || cache.isTimeout(mapping.getMappingMetadataTimeout())) {
            synchronized (this) {
                cache = esFieldTypes.get(key);
                if (cache == null || cache.isTimeout(mapping.getMappingMetadataTimeout())) {
                    MappingMetaData mappingMetaData = esConnection.getMapping(mapping.get_index());
                    if (mappingMetaData == null) {
                        throw new IllegalArgumentException("Not found the mapping info of index: " + mapping.get_index());
                    }

                    Map<String, String> fieldType = new LinkedHashMap<>();

                    Map<String, Object> sourceMap = mappingMetaData.getSourceAsMap();
                    Map<String, Object> esMapping = (Map<String, Object>) sourceMap.get("properties");
                    for (Map.Entry<String, Object> entry : esMapping.entrySet()) {
                        Map<String, Object> value = (Map<String, Object>) entry.getValue();
                        if (value.containsKey("properties")) {
                            fieldType.put(entry.getKey(), "object");
                        } else {
                            fieldType.put(entry.getKey(), (String) value.get("type"));
                        }
                    }
                    esFieldTypes.put(key, cache = new EsFieldTypesCache(fieldType));
                }
            }
        }
        return cache.fieldTypes.get(fieldName);
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

    private void addRequest(ESBulkRequest.ESIndexRequest indexRequest, BulkRequestList bulkRequestList) {
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

    public static class BulkRequestListImpl implements BulkRequestList {
        private final List<ESBulkRequest.ESRequest> requests = new ArrayList<>();

        @Override
        public void add(ESBulkRequest.ESRequest request) {
            synchronized (requests) {
                requests.add(request);
            }
        }

        List<ESBulkRequest.ESRequest> drainTo() {
            synchronized (requests) {
                ArrayList<ESBulkRequest.ESRequest> esRequests = new ArrayList<>(requests);
                requests.clear();
                return esRequests;
            }
        }

        @Override
        public boolean isEmpty() {
            return requests.isEmpty();
        }

        @Override
        public int size() {
            return requests.size();
        }

        @Override
        public String toString() {
            return "BulkRequestListImpl{" +
                    "size=" + requests.size() +
                    '}';
        }
    }

    // es 字段类型本地缓存
    public static class EsFieldTypesCache {
        private final Map<String, String> fieldTypes;
        private final long timestamp = System.currentTimeMillis();

        public EsFieldTypesCache(Map<String, String> fieldTypes) {
            this.fieldTypes = fieldTypes;
        }

        public boolean isTimeout(long timeout) {
            return System.currentTimeMillis() - timestamp > timeout;
        }
    }
}
