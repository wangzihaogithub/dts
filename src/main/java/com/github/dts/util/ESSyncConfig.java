package com.github.dts.util;

import com.github.dts.impl.elasticsearch.ESAdapter;
import com.github.dts.impl.elasticsearch.NestedFieldWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.AntPathMatcher;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * ES 映射配置
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncConfig {
    public static final String ES_ID_FIELD_NAME = "_id";
    private static final Logger log = LoggerFactory.getLogger(ESSyncConfig.class);
    private String dataSourceKey;   // 数据源key
    private String destination;     // canal destination
    private String[] adapterNamePattern;
    private ESMapping esMapping;
    private String md5;

    public static String getEsSyncConfigKey(String destination, String database, String table) {
        return destination + "_" + database + "_" + table;
    }

    public static void loadESSyncConfig(Map<String, Map<String, ESSyncConfig>> map,
                                        Map<String, ESSyncConfig> configMap,
                                        Properties envProperties, CanalConfig.CanalAdapter canalAdapter,
                                        String adapterName,
                                        File resourcesDir, String env) {
        Map<String, ESSyncConfig> load = loadYamlToBean(envProperties, canalAdapter, resourcesDir, env);
        for (Map.Entry<String, ESSyncConfig> entry : load.entrySet()) {
            ESSyncConfig config = entry.getValue();
            if (!config.getEsMapping().isEnable()) {
                continue;
            }
            if (!config.isMatchAdapterName(adapterName)) {
                continue;
            }

            String configName = entry.getKey();
            configMap.put(configName, config);
            String schema = CanalConfig.DatasourceConfig.getCatalog(config.getDataSourceKey());
            for (SchemaItem.TableItem item : config.getEsMapping().getSchemaItem().getAliasTableItems().values()) {
                map.computeIfAbsent(getEsSyncConfigKey(config.getDestination(), schema, item.getTableName()),
                        k -> new ConcurrentHashMap<>()).put(configName, config);
            }
            for (Map.Entry<String, ObjectField> e : config.getEsMapping().getObjFields().entrySet()) {
                ObjectField v = e.getValue();
                if (v == null) {
                    continue;
                }
                ObjectField.ParamSql paramSql = v.getParamSql();
                if (paramSql == null) {
                    continue;
                }
                SchemaItem schemaItem = paramSql.getSchemaItem();
                if (schemaItem == null || ESSyncUtil.isEmpty(schemaItem.getAliasTableItems())) {
                    continue;
                }
                for (SchemaItem.TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    map.computeIfAbsent(getEsSyncConfigKey(config.getDestination(), schema, tableItem.getTableName()),
                                    k -> new ConcurrentHashMap<>())
                            .put(configName, config);
                }
            }
        }
    }

    public static Map<String, ESSyncConfig> loadYamlToBean(Properties envProperties, CanalConfig.CanalAdapter canalAdapter, File resourcesDir, String env) {
        log.info("## Start loading es mapping config {}", resourcesDir);
        Map<String, ESSyncConfig> esSyncConfig = new LinkedHashMap<>();
        Map<String, byte[]> yamlMap = ESSyncUtil.loadYamlToBytes(resourcesDir);
        for (Map.Entry<String, byte[]> entry : yamlMap.entrySet()) {
            String fileName = entry.getKey();
            byte[] content = entry.getValue();

            ESSyncConfig config = YmlConfigBinder.bindYmlToObj(null, content, ESSyncConfig.class, envProperties);
            if (config == null) {
                continue;
            }
            String[] destination = canalAdapter.getDestination();
            if (ESSyncUtil.isEmpty(config.getDestination())) {
                if (destination == null || destination.length == 0) {
                    config.setDestination("");
                } else if (destination.length == 1) {
                    config.setDestination(destination[0]);
                }
            }
            if (!Objects.equals(env, config.getEsMapping().getEnv())) {
                continue;
            }
            String md5 = Util.md5(content);
            try {
                config.init(md5);
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + fileName + " " + e, e);
            }
            esSyncConfig.put(fileName, config);
        }

        log.info("## ES mapping config loaded");
        return esSyncConfig;
    }

    @Override
    public String toString() {
        return dataSourceKey + "." + destination + "." + esMapping;
    }

    public String getMd5() {
        return md5;
    }

    private void init(String md5) {
        if (Util.isBlank(esMapping._index)) {
            throw new NullPointerException("empty esMapping._index");
        }
        if (Util.isBlank(esMapping._id) && Util.isBlank(esMapping.getPk())) {
            throw new NullPointerException("empty esMapping._id or esMapping.pk");
        }
        if (Util.isBlank(esMapping.sql)) {
            throw new NullPointerException("empty esMapping.sql");
        }
        if (Util.isBlank(esMapping._id)) {
            esMapping._id = esMapping.pk;
        } else if (Util.isBlank(esMapping.pk)) {
            esMapping.pk = esMapping._id;
        }
        this.md5 = md5;
        esMapping.setConfig(this);

        SchemaItem schemaItem = SqlParser.parse(esMapping.getSql());
        esMapping.setSchemaItem(schemaItem);
        schemaItem.init(null, esMapping);
        if (schemaItem.getAliasTableItems().isEmpty() || schemaItem.getSelectFields().isEmpty()) {
            throw new IllegalArgumentException("table fields is empty, Parse sql error" + esMapping.getSql());
        }

        for (Map.Entry<String, ObjectField> entry : esMapping.getObjFields().entrySet()) {
            ObjectField objectField = entry.getValue();
            objectField.fieldName = entry.getKey();
            objectField.esMapping = esMapping;
            ObjectField.Type type = objectField.getType();
            if (type.isSqlType()) {
                ObjectField.ParamSql paramSql = objectField.getParamSql();
                if (paramSql == null) {
                    throw new IllegalArgumentException("fieldName = " + objectField.fieldName + ", sql type paramSql is null");
                }
                paramSql.init(objectField);
                if (type.isFlatSqlType()) {
                    if (paramSql.getSchemaItem().getSelectFields().size() != 1) {
                        throw new IllegalArgumentException("fieldName = " + objectField.fieldName + ", " + type + " must only single select field!");
                    }
                }
            } else if (type.isLlmVector()) {
                ObjectField.ParamLlmVector paramLlmVector = objectField.getParamLlmVector();
                if (paramLlmVector == null) {
                    throw new IllegalArgumentException("fieldName = " + objectField.fieldName + ", llmVector type paramLlmVector is null");
                }
                paramLlmVector.init(objectField);
            } else if (type == ObjectField.Type.ARRAY) {
                ObjectField.ParamArray paramArray = objectField.getParamArray();
                if (paramArray == null || paramArray.getSplit() == null) {
                    throw new IllegalArgumentException("fieldName = " + objectField.fieldName + ", paramArray type paramArray split is null");
                }
            } else if (type == ObjectField.Type.STATIC_METHOD) {
                ObjectField.ParamStaticMethod paramStaticMethod = objectField.getParamStaticMethod();
                if (paramStaticMethod == null || Util.isBlank(paramStaticMethod.getMethod())) {
                    throw new IllegalArgumentException("fieldName = " + objectField.fieldName + ", paramStaticMethod type method is empty");
                }
                paramStaticMethod.init();
            }

        }
    }

    public String getDataSourceKey() {
        return dataSourceKey;
    }

    public void setDataSourceKey(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public ESMapping getEsMapping() {
        return esMapping;
    }

    public void setEsMapping(ESMapping esMapping) {
        this.esMapping = esMapping;
    }

    public String[] getAdapterNamePattern() {
        return adapterNamePattern;
    }

    public void setAdapterNamePattern(String[] adapterNamePattern) {
        this.adapterNamePattern = adapterNamePattern;
    }

    public boolean isMatchAdapterName(String adapterName) {
        String[] adapterNamePattern = this.adapterNamePattern;
        if (adapterNamePattern == null || adapterNamePattern.length == 0) {
            return true;
        }
        AntPathMatcher matcher = null;
        for (String s : adapterNamePattern) {
            if (s.equals("*")) {
                return true;
            } else if (Objects.equals(s, adapterName)) {
                return true;
            } else {
                if (matcher == null) {
                    matcher = new AntPathMatcher("-");
                }
                if (matcher.match(s, adapterName)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static class ESMapping {
        private String env;
        private String _index;
        private String _id;
        private String pk;
        private long mappingMetadataTimeout = 12L * 60L * 60000L;
        private int retryOnConflict = 5;
        private boolean upsert = false;
        /**
         * 遇到null，是否写入
         */
        private boolean writeNull = false;
        private String indexUpdatedTime = "";
        private boolean updateByQuerySkipIndexUpdatedTime = true;
        private int version = 0;
        private boolean enable = true;

        private String sql;
        // 对象字段, 例: objFields:
        // - _labels: array:;
        private Map<String, ObjectField> objFields = new LinkedHashMap<>();
        private SchemaItem schemaItem;                             // sql解析结果模型
        private ESSyncConfig config;

        @Override
        public String toString() {
            return env + "[" + _index + "]";
        }

        public int getRetryOnConflict() {
            return retryOnConflict;
        }

        public void setRetryOnConflict(int retryOnConflict) {
            this.retryOnConflict = retryOnConflict;
        }

        public boolean isUpdateByQuerySkipIndexUpdatedTime() {
            return updateByQuerySkipIndexUpdatedTime;
        }

        public void setUpdateByQuerySkipIndexUpdatedTime(boolean updateByQuerySkipIndexUpdatedTime) {
            this.updateByQuerySkipIndexUpdatedTime = updateByQuerySkipIndexUpdatedTime;
        }

        public long getMappingMetadataTimeout() {
            return mappingMetadataTimeout;
        }

        public void setMappingMetadataTimeout(long mappingMetadataTimeout) {
            this.mappingMetadataTimeout = mappingMetadataTimeout;
        }

        public String getIndexUpdatedTime() {
            return indexUpdatedTime;
        }

        public void setIndexUpdatedTime(String indexUpdatedTime) {
            this.indexUpdatedTime = indexUpdatedTime;
        }

        public boolean isSetIndexUpdatedTime() {
            return indexUpdatedTime != null && !indexUpdatedTime.isEmpty();
        }

        public boolean isWriteNull() {
            return writeNull;
        }

        public void setWriteNull(boolean writeNull) {
            this.writeNull = writeNull;
        }

        public String getEnv() {
            return env;
        }

        public void setEnv(String env) {
            this.env = env;
        }

        public boolean isEnable() {
            return enable;
        }

        public void setEnable(boolean enable) {
            this.enable = enable;
        }

        public ESSyncConfig getConfig() {
            return config;
        }

        public void setConfig(ESSyncConfig config) {
            this.config = config;
        }

        public String get_index() {
//            return CanalAdapterApplication.getENV() + "-" + _index;
            return _index;
        }

        public void set_index(String _index) {
            this._index = _index;
        }

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public String get_id() {
            return _id;
        }

        public void set_id(String _id) {
            this._id = _id;
        }

        public boolean isUpsert() {
            return upsert;
        }

        public void setUpsert(boolean upsert) {
            this.upsert = upsert;
        }

        public String getPk() {
            return pk;
        }

        public void setPk(String pk) {
            this.pk = pk;
        }

        public Map<String, ObjectField> getObjFields() {
            return objFields;
        }

        public void setObjFields(Map<String, ObjectField> objFields) {
            this.objFields = objFields;
        }

        public ObjectField getObjectField(String parentFieldName, String name) {
            if (parentFieldName != null && !parentFieldName.isEmpty()) {
                return objFields.get(parentFieldName + "$" + name);
            } else {
                return objFields.get(name);
            }
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public SchemaItem getSchemaItem() {
            return schemaItem;
        }

        public void setSchemaItem(SchemaItem schemaItem) {
            this.schemaItem = schemaItem;
        }
    }

    public static class ObjectField {
        /**
         * array
         * object
         * object-sql
         * array-sql
         * object-flat-sql
         * array-flat-sql
         * boolean
         * static-method
         * llm-vector
         */
        private Type type;
        private String fieldName;
        private ESMapping esMapping;

        private ParamStaticMethod paramStaticMethod;
        private ParamArray paramArray;
        private ParamSql paramSql;
        private ParamLlmVector paramLlmVector;

        @Override
        public String toString() {
            return type + "(" + fieldName + ")";
        }

        /**
         * 转换为ES对象
         * <p>
         * |ARRAY   |OBJECT     |ARRAY_SQL      |OBJECT_SQL      |
         * |数组     | 对象      |数组sql查询多条  |对象sql查询单条   |
         * <p>
         * 该方法只实现 ARRAY与OBJECT
         * ARRAY_SQL与OBJECT_SQL的实现 - {@link NestedFieldWriter}
         *
         * @param val     val
         * @param mapping mapping
         * @param row     row
         * @return ES对象
         * @see ESSyncServiceListener#onSyncAfter(List, ESAdapter, ESTemplate.BulkRequestList)
         */
        public Object parse(Object val, ESMapping mapping, Map<String, Object> row) {
            switch (type) {
                case ARRAY: {
                    if (val == null) {
                        return null;
                    }
                    if (val instanceof Collection) {
                        return val;
                    }
                    String varStr = val.toString();
                    if (Util.isEmpty(varStr)) {
                        return null;
                    }
                    String[] values = varStr.split(paramArray.split);
                    return Arrays.asList(values);
                }
                case OBJECT: {
                    if (val == null) {
                        return null;
                    }
                    if (val instanceof Map) {
                        return val;
                    }
                    return JsonUtil.toMap(val.toString(), true);
                }
                case BOOLEAN: {
                    if (val == null) {
                        return null;
                    }
                    if (val instanceof Boolean) {
                        return val;
                    }
                    return ESSyncUtil.castToBoolean(val);
                }
                case LLM_VECTOR: {
                    String string;
                    if (val == null) {
                        return null;
                    } else if ((string = val.toString().trim()).isEmpty()) {
                        return null;
                    } else {
                        return paramLlmVector.getTypeLlmVectorAPI().vector(string);
                    }
                }
                case STATIC_METHOD: {
                    if (val == null) {
                        return null;
                    }
                    if (paramStaticMethod.staticMethodAccessor == null) {
                        paramStaticMethod.init();
                    }
                    String[] split1 = fieldName.split("\\$");
                    String parentFieldName;
                    String fieldName;
                    if (split1.length == 1) {
                        fieldName = split1[0];
                        parentFieldName = null;
                    } else {
                        parentFieldName = split1[0];
                        fieldName = split1[1];
                    }
                    return paramStaticMethod.staticMethodAccessor.apply(new ESStaticMethodParam(val, mapping, fieldName, parentFieldName));
                }
                case ARRAY_SQL:
                case OBJECT_SQL:
                case ARRAY_FLAT_SQL:
                case OBJECT_FLAT_SQL:
                default: {
                    return val;
                }
            }
        }

        public ParamSql getParamSql() {
            return paramSql;
        }

        public void setParamSql(ParamSql paramSql) {
            this.paramSql = paramSql;
        }

        public ParamArray getParamArray() {
            return paramArray;
        }

        public void setParamArray(ParamArray paramArray) {
            this.paramArray = paramArray;
        }

        public ParamLlmVector getParamLlmVector() {
            return paramLlmVector;
        }

        public void setParamLlmVector(ParamLlmVector paramLlmVector) {
            this.paramLlmVector = paramLlmVector;
        }

        public ParamStaticMethod getParamStaticMethod() {
            return paramStaticMethod;
        }

        public void setParamStaticMethod(ParamStaticMethod paramStaticMethod) {
            this.paramStaticMethod = paramStaticMethod;
        }

        public String getFieldName() {
            return fieldName;
        }

        public ESMapping getEsMapping() {
            return esMapping;
        }

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public boolean isSqlType() {
            return type != null && type.isSqlType();
        }

        public enum Type {
            /**
             * 数组(逗号分割，任意分隔符分割),
             */
            ARRAY,
            /**
             * 对象(JSON.parse)
             */
            OBJECT,
            /**
             * 数组(sql多条查询，一对多关系),List《Map》类型
             */
            ARRAY_SQL,
            /**
             * 对象(sql单条查询，一对一关系)《Map》类型
             */
            OBJECT_SQL,
            /**
             * 对象(sql单条查询，一对一关系),List《String｜Number|Boolean|基本数据》类型
             */
            OBJECT_FLAT_SQL,
            /**
             * 数组(sql多条查询，一对多关系),List《String｜Number|Boolean|基本数据》类型
             */
            ARRAY_FLAT_SQL,
            /**
             * boolean
             */
            BOOLEAN,
            /**
             * 自定义逻辑：静态方法
             */
            STATIC_METHOD,
            /**
             * 使用大模型，向量化字段
             */
            LLM_VECTOR;

            public boolean isSqlType() {
                return isObjectSqlType() || isArraySqlType();
            }

            public boolean isObjectSqlType() {
                return this == Type.OBJECT_SQL || this == Type.OBJECT_FLAT_SQL;
            }

            public boolean isArraySqlType() {
                return this == Type.ARRAY_SQL || this == Type.ARRAY_FLAT_SQL;
            }

            public boolean isFlatSqlType() {
                return this == Type.OBJECT_FLAT_SQL || this == Type.ARRAY_FLAT_SQL;
            }

            public boolean isLlmVector() {
                return this == Type.LLM_VECTOR;
            }

            public boolean isSingleJoinType() {
                return this == Type.OBJECT_SQL;
            }
        }

        public static class ParamArray {
            private String split;

            public String getSplit() {
                return split;
            }

            public void setSplit(String split) {
                this.split = split;
            }
        }

        public static class ParamSql {
            private String sql;
            private String onMainTableChangeWhereSql;
            private String onSlaveTableChangeWhereSql;
            /**
             * 默认取sql中的这个 #{xxx} 名
             */
            private String joinTableColumnName;

            private transient SchemaItem schemaItem;

            private void init(ObjectField objectField) {
                if (!Util.isBlank(sql)) {
                    SchemaItem schemaItem1 = SqlParser.parse(sql);
                    schemaItem1.init(objectField, objectField.esMapping);
                    if (schemaItem1.getAliasTableItems().isEmpty()) {
                        throw new IllegalArgumentException("field " + objectField + ", miss AliasTable! " + sql);
                    }
                    if (schemaItem1.getSelectFields().isEmpty()) {
                        throw new IllegalArgumentException("field " + objectField + ", miss SelectFields! " + sql);
                    }
                    this.schemaItem = schemaItem1;
                }
                if (Util.isBlank(joinTableColumnName)) {
                    this.joinTableColumnName = joinTableColumnName();
                }
                if (SqlParser.existSelectLimit(sql)) {
                    throw new IllegalArgumentException("field " + objectField + ", no support limit! " + sql);
                }
                if (SqlParser.existInjectLimit(onMainTableChangeWhereSql)) {
                    throw new IllegalArgumentException("field " + objectField + ", no support limit! " + onMainTableChangeWhereSql);
                }
                if (SqlParser.existInjectGroupBy(onMainTableChangeWhereSql)) {
                    throw new IllegalArgumentException("field " + objectField + ", no support group by! " + onMainTableChangeWhereSql);
                }
                if (SqlParser.existInjectLimit(onSlaveTableChangeWhereSql)) {
                    throw new IllegalArgumentException("field " + objectField + ", no support limit! " + onSlaveTableChangeWhereSql);
                }
                if (SqlParser.existInjectGroupBy(onSlaveTableChangeWhereSql)) {
                    throw new IllegalArgumentException("field " + objectField + ", no support group by! " + onSlaveTableChangeWhereSql);
                }
            }

            public String getSql() {
                return sql;
            }

            public void setSql(String sql) {
                this.sql = sql;
            }

            public String sql() {
                return SqlParser.removeGroupBy(sql);
            }

            @Deprecated
            public void setParentDocumentId(String joinTableColumnName) {
                this.joinTableColumnName = joinTableColumnName;
            }

            @Deprecated
            public void setOnChildChangeWhereSql(String onSlaveTableChangeWhereSql) {
                this.onSlaveTableChangeWhereSql = onSlaveTableChangeWhereSql;
            }

            @Deprecated
            public void setOnParentChangeWhereSql(String onMainTableChangeWhereSql) {
                this.onMainTableChangeWhereSql = onMainTableChangeWhereSql;
            }

            public SchemaItem getSchemaItem() {
                return schemaItem;
            }

            public String getJoinTableColumnName() {
                return joinTableColumnName;
            }

            public void setJoinTableColumnName(String joinTableColumnName) {
                this.joinTableColumnName = joinTableColumnName;
            }

            public String[] groupByIdColumns() {
                return SqlParser.getGroupByIdColumns(sql);
            }

            String joinTableColumnName() {
                SchemaItem.TableItem mainTable = schemaItem.getMainTable();
                List<String> mainColumnList = SqlParser.getVarColumnList(onSlaveTableChangeWhereSql).stream()
                        .filter(e -> e.isOwner(mainTable.getAlias()))
                        .map(SqlParser.BinaryOpExpr::getName)
                        .collect(Collectors.toList());
                if (mainColumnList.isEmpty()) {
                    return null;
                }
                LinkedHashSet<String> mainColumnSet = new LinkedHashSet<>(mainColumnList);
                if (mainColumnSet.size() != 1) {
                    throw new IllegalArgumentException("joinTableColumnName is only support single var column. find " + mainColumnSet);
                }
                return mainColumnSet.iterator().next();
            }

            public String getOnSlaveTableChangeWhereSql() {
                return onSlaveTableChangeWhereSql;
            }

            public void setOnSlaveTableChangeWhereSql(String onSlaveTableChangeWhereSql) {
                this.onSlaveTableChangeWhereSql = onSlaveTableChangeWhereSql;
            }

            public String getOnMainTableChangeWhereSql() {
                return onMainTableChangeWhereSql;
            }

            public void setOnMainTableChangeWhereSql(String onMainTableChangeWhereSql) {
                this.onMainTableChangeWhereSql = onMainTableChangeWhereSql;
            }

            public String getFullSql(boolean isMainTable) {
                String sql1 = sql();
                if (isMainTable) {
                    return sql1 + " " + onMainTableChangeWhereSql;
                } else {
                    return sql1 + " " + onSlaveTableChangeWhereSql;
                }
            }

        }

        public static class ParamStaticMethod {
            private String method;
            private transient StaticMethodAccessor<ESStaticMethodParam> staticMethodAccessor;

            public void init() {
                staticMethodAccessor = new StaticMethodAccessor<>(method, ESStaticMethodParam.class);
            }

            public String getMethod() {
                return method;
            }

            public void setMethod(String method) {
                this.method = method;
            }
        }

        /**
         * Dense vector field type 密集向量字段类型
         * https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html#dense-vector-params
         */
        public static class ParamLlmVector {
            private Class<? extends LlmEmbeddingModel> modelClass = OpenAiLlmEmbeddingModel.class;
            private String apiKey;
            private String baseUrl;
            private String modelName;
            /**
             * （可选，整数）向量维度数
             */
            private Integer dimensions = null;

            /**
             * etl刷数据时，判断是否相等的字段
             */
            private String etlEqualsFieldName;
            /**
             * 每次请求最多几条
             */
            private int requestMaxContentSize = 6;
            /**
             * 每分钟调用次数（QPM）
             * 阿里云：为了保证用户调用模型的公平性，百炼设置了基础限流。
             * 限流是基于模型维度的，并且和调用用户的阿里云主账号相关联，按照该账号下所有API-KEY调用该模型的总和计算限流。
             * 如果超出调用限制，用户的API请求将会因为限流而失败，用户需等到不满足限流条件时才能再次调用。
             */
            private int qpm = 60;
            /**
             * 使用队列名称
             */
            private String requestQueueName;

            private volatile transient TypeLlmVectorAPI typeLlmVectorAPI;

            private void init(ObjectField objectField) {
                if (Util.isBlank(requestQueueName)) {
                    /*
                     * 加队列：每分钟调用次数（QPM）
                     * 阿里云：为了保证用户调用模型的公平性，百炼设置了基础限流。
                     * 限流是基于模型维度的，并且和调用用户的阿里云主账号相关联，按照该账号下所有API-KEY调用该模型的总和计算限流。
                     * 如果超出调用限制，用户的API请求将会因为限流而失败，用户需等到不满足限流条件时才能再次调用。
                     */
                    requestQueueName = apiKey + "_" + modelName;
                }
                if (Util.isBlank(etlEqualsFieldName)) {
                    SchemaItem.FieldItem fieldItem = etlEqualsFieldName(objectField);
                    if (fieldItem == null) {
                        throw new IllegalArgumentException("etlEqualsFieldName must not empty!");
                    } else {
                        this.etlEqualsFieldName = fieldItem.getFieldName();
                    }
                }
                getTypeLlmVectorAPI();
            }

            public int getQpm() {
                return qpm;
            }

            public void setQpm(int qpm) {
                this.qpm = qpm;
            }

            public String getRequestQueueName() {
                return requestQueueName;
            }

            public void setRequestQueueName(String requestQueueName) {
                this.requestQueueName = requestQueueName;
            }

            public Class<? extends LlmEmbeddingModel> getModelClass() {
                return modelClass;
            }

            public void setModelClass(Class<? extends LlmEmbeddingModel> modelClass) {
                this.modelClass = modelClass;
            }

            public int getRequestMaxContentSize() {
                return requestMaxContentSize;
            }

            public void setRequestMaxContentSize(int requestMaxContentSize) {
                this.requestMaxContentSize = requestMaxContentSize;
            }

            public boolean isContentSizeThreshold(int contentSize) {
                return contentSize >= requestMaxContentSize;
            }

            private SchemaItem.FieldItem etlEqualsFieldName(ObjectField objectField) {
                ESMapping esMapping = objectField.esMapping;
                SchemaItem.FieldItem currFieldItem = esMapping.getSchemaItem().getSelectFields().get(objectField.fieldName);
                List<SchemaItem.FieldItem> fieldItemList = esMapping.getSchemaItem().selectField(currFieldItem);
                for (int i = 0; i < fieldItemList.size(); i++) {
                    SchemaItem.FieldItem fieldItem = fieldItemList.get(i);
                    if (fieldItem == currFieldItem) {
                        if (i + 1 < fieldItemList.size()) {
                            return fieldItemList.get(i + 1);
                        } else if (i - 1 >= 0) {
                            return fieldItemList.get(i - 1);
                        }
                    }
                }
                return null;
            }

            @Override
            public String toString() {
                return modelClass + "[" + modelName + "]";
            }

            public TypeLlmVectorAPI getTypeLlmVectorAPI() {
                if (typeLlmVectorAPI == null) {
                    synchronized (this) {
                        if (typeLlmVectorAPI == null) {
                            try {
                                typeLlmVectorAPI = new TypeLlmVectorAPI(this);
                            } catch (Exception e) {
                                Util.sneakyThrows(e);
                            }
                        }
                    }
                }
                return typeLlmVectorAPI;
            }

            public String getEtlEqualsFieldName() {
                return etlEqualsFieldName;
            }

            public void setEtlEqualsFieldName(String refTextFieldName) {
                this.etlEqualsFieldName = refTextFieldName;
            }

            public Integer getDimensions() {
                return dimensions;
            }

            public void setDimensions(Integer dimensions) {
                this.dimensions = dimensions;
            }

            public String getApiKey() {
                return apiKey;
            }

            public void setApiKey(String apiKey) {
                this.apiKey = apiKey;
            }

            public String getBaseUrl() {
                return baseUrl;
            }

            public void setBaseUrl(String baseUrl) {
                this.baseUrl = baseUrl;
            }

            public String getModelName() {
                return modelName;
            }

            public void setModelName(String modelName) {
                this.modelName = modelName;
            }

            public enum LlmVectorType {
                openAi
            }
        }
    }
}
