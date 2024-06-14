package com.github.dts.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ES 映射配置
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncConfig {
    private String dataSourceKey;   // 数据源key

    private String outerAdapterKey; // adapter key

    private String groupId;         // group id

    private String destination;     // canal destination

    private ESMapping esMapping;

    public void init() {
        if (esMapping._index == null) {
            throw new NullPointerException("esMapping._index");
        }
        if (esMapping._id == null && esMapping.getPk() == null) {
            throw new NullPointerException("esMapping._id or esMapping.pk");
        }
        if (esMapping.sql == null) {
            throw new NullPointerException("esMapping.sql");
        }

        esMapping.setConfig(this);
        for (Map.Entry<String, ObjectField> entry : esMapping.getObjFields().entrySet()) {
            ObjectField objectField = entry.getValue();
            objectField.setEsMapping(esMapping);
            objectField.setFieldName(entry.getKey());
            if (objectField.getSql() != null && !objectField.getSql().isEmpty()) {
                SchemaItem schemaItem = SqlParser.parse(objectField.getSql());
                schemaItem.setObjectField(objectField);
                objectField.setSchemaItem(schemaItem);
            }
        }
        SchemaItem schemaItem = SqlParser.parse(esMapping.getSql());
        esMapping.setSchemaItem(schemaItem);
    }

    public String getDataSourceKey() {
        return dataSourceKey;
    }

    public void setDataSourceKey(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    public String getOuterAdapterKey() {
        return outerAdapterKey;
    }

    public void setOuterAdapterKey(String outerAdapterKey) {
        this.outerAdapterKey = outerAdapterKey;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
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

    public static class ESMapping {
        private String env;
        private String _index;
        private String _id;
        private long mappingMetadataTimeout = 12L* 60L * 60000L;
        private boolean upsert = false;
        /**
         * 遇到null，是否写入
         */
        private boolean writeNull = false;
        private String indexUpdatedTime = "";
        private int version = 0;
        private boolean enable = true;
        private String pk;
        private Map<String, RelationMapping> relations = new LinkedHashMap<>();
        private String sql;
        // 对象字段, 例: objFields:
        // - _labels: array:;
        private Map<String, ObjectField> objFields = new LinkedHashMap<>();
        private List<String> skips = new ArrayList<>();
        private int commitBatch = 1000;
        private String etlCondition;
        private boolean syncByTimestamp = false;                // 是否按时间戳定时同步
        private boolean detectNoop = true;                     // 文档无变化时是否刷新文档（同步词库的时候，有时文档无变化，那么索引可能不会重建）
        private Long syncInterval;                           // 同步时间间隔

        private SchemaItem schemaItem;                             // sql解析结果模型
        private ESSyncConfig config;

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

        public boolean isDetectNoop() {
            return detectNoop;
        }

        public void setDetectNoop(boolean detectNoop) {
            this.detectNoop = detectNoop;
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

        public List<String> getSkips() {
            return skips;
        }

        public void setSkips(List<String> skips) {
            this.skips = skips;
        }

        public Map<String, RelationMapping> getRelations() {
            return relations;
        }

        public void setRelations(Map<String, RelationMapping> relations) {
            this.relations = relations;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public int getCommitBatch() {
            return commitBatch;
        }

        public void setCommitBatch(int commitBatch) {
            this.commitBatch = commitBatch;
        }

        public String getEtlCondition() {
            return etlCondition;
        }

        public void setEtlCondition(String etlCondition) {
            this.etlCondition = etlCondition;
        }

        public Long getSyncInterval() {
            return syncInterval;
        }

        public void setSyncInterval(Long syncInterval) {
            this.syncInterval = syncInterval;
        }

        public boolean isSyncByTimestamp() {
            return syncByTimestamp;
        }

        public void setSyncByTimestamp(boolean syncByTimestamp) {
            this.syncByTimestamp = syncByTimestamp;
        }

        public SchemaItem getSchemaItem() {
            return schemaItem;
        }

        public void setSchemaItem(SchemaItem schemaItem) {
            this.schemaItem = schemaItem;
        }
    }

    public static class RelationMapping {

        private String name;
        private String parent;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getParent() {
            return parent;
        }

        public void setParent(String parent) {
            this.parent = parent;
        }
    }

    public static class ObjectField {
        /**
         * array
         * object
         * object-sql
         * array-sql
         */
        private Type type;
        private String fieldName;
        private String sql;
        private String method;
        private String split = "";
        private String onParentChangeWhereSql;
        private String onChildChangeWhereSql;
        private String parentDocumentId;
        private SchemaItem schemaItem;
        private ESMapping esMapping;

        private transient StaticMethodAccessor<ESStaticMethodParam> staticMethodAccessor;

        public StaticMethodAccessor<ESStaticMethodParam> staticMethodAccessor() {
            if (staticMethodAccessor == null) {
                staticMethodAccessor = new StaticMethodAccessor<>(method, ESStaticMethodParam.class);
            }
            return staticMethodAccessor;
        }

        public String getMethod() {
            return method;
        }

        public void setMethod(String method) {
            this.method = method;
        }

        public String getParentDocumentId() {
            return parentDocumentId;
        }

        public void setParentDocumentId(String parentDocumentId) {
            this.parentDocumentId = parentDocumentId;
        }

        public String getOnChildChangeWhereSql() {
            return onChildChangeWhereSql;
        }

        public void setOnChildChangeWhereSql(String onChildChangeWhereSql) {
            this.onChildChangeWhereSql = onChildChangeWhereSql;
        }

        public String getOnParentChangeWhereSql() {
            return onParentChangeWhereSql;
        }

        public void setOnParentChangeWhereSql(String onParentChangeWhereSql) {
            this.onParentChangeWhereSql = onParentChangeWhereSql;
        }

        public String getFullSql(boolean isParentChange) {
            if (isParentChange) {
                return sql + " " + onParentChangeWhereSql;
            } else {
                return sql + " " + onChildChangeWhereSql;
            }
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public ESMapping getEsMapping() {
            return esMapping;
        }

        public void setEsMapping(ESMapping esMapping) {
            this.esMapping = esMapping;
        }

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public String getSplit() {
            return split;
        }

        public void setSplit(String split) {
            this.split = split;
        }

        public SchemaItem getSchemaItem() {
            return schemaItem;
        }

        public void setSchemaItem(SchemaItem schemaItem) {
            this.schemaItem = schemaItem;
        }

        public enum Type {
            /**
             * 数组(逗号分割), 对象(JSON.parse), 数组(sql多条查询), 对象(sql单条查询) ,boolean
             */
            ARRAY, OBJECT, ARRAY_SQL, OBJECT_SQL, BOOLEAN, STATIC_METHOD, URL
        }
    }
}
