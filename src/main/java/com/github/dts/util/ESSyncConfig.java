package com.github.dts.util;

import com.github.dts.impl.elasticsearch7x.ES7xAdapter;
import com.github.dts.impl.elasticsearch7x.NestedFieldWriter;

import java.util.*;
import java.util.stream.Collectors;

/**
 * ES 映射配置
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncConfig {
    public static final String ES_ID_FIELD_NAME = "_id";
    private String dataSourceKey;   // 数据源key
    private String destination;     // canal destination
    private ESMapping esMapping;
    private String md5;

    @Override
    public String toString() {
        return dataSourceKey + "." + destination + "." + esMapping;
    }

    public String getMd5() {
        return md5;
    }

    public void init(String md5) {
        if (esMapping._index == null) {
            throw new NullPointerException("esMapping._index");
        }
        if (esMapping._id == null && esMapping.getPk() == null) {
            throw new NullPointerException("esMapping._id or esMapping.pk");
        }
        if (esMapping.sql == null) {
            throw new NullPointerException("esMapping.sql");
        }
        this.md5 = md5;
        esMapping.setConfig(this);

        SchemaItem schemaItem = SqlParser.parse(esMapping.getSql());
        esMapping.setSchemaItem(schemaItem);
        schemaItem.init(null, esMapping);
        if (schemaItem.getAliasTableItems().isEmpty() || schemaItem.getSelectFields().isEmpty()) {
            throw new RuntimeException("Parse sql error" + esMapping.getSql());
        }

        for (Map.Entry<String, ObjectField> entry : esMapping.getObjFields().entrySet()) {
            ObjectField objectField = entry.getValue();
            objectField.setEsMapping(esMapping);
            objectField.setFieldName(entry.getKey());
            String sql = objectField.getSql();
            if (sql != null && !sql.isEmpty()) {
                SchemaItem schemaItem1 = SqlParser.parse(objectField.sql());
                schemaItem1.init(objectField, esMapping);
                if (schemaItem1.getAliasTableItems().isEmpty() || schemaItem1.getSelectFields().isEmpty()) {
                    throw new RuntimeException("Parse sql error" + sql);
                }
                objectField.setSchemaItem(schemaItem1);
            }
            if (Util.isBlank(objectField.getJoinTableColumnName())) {
                objectField.setJoinTableColumnName(objectField.joinTableColumnName());
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

    public static class ESMapping {
        private String env;
        private String _index;
        private String _id;
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
        private String pk;
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
         */
        private Type type;

        private String fieldName;
        private String sql;
        private String method;
        private String split = "";
        private String onMainTableChangeWhereSql;
        private String onSlaveTableChangeWhereSql;
        /**
         * 取这个 #{xxx}
         */
        private String joinTableColumnName;
        private SchemaItem schemaItem;
        private ESMapping esMapping;
        private transient StaticMethodAccessor<ESStaticMethodParam> staticMethodAccessor;

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
         * @return ES对象
         * @see ESSyncServiceListener#onSyncAfter(List, ES7xAdapter, ESTemplate.BulkRequestList)
         */
        public Object parse(Object val, ESMapping mapping) {
            if (val == null) {
                return null;
            }

            switch (type) {
                case ARRAY: {
                    if (val instanceof Collection) {
                        return val;
                    }
                    String varStr = val.toString();
                    if (Util.isEmpty(varStr)) {
                        return null;
                    }
                    String[] values = varStr.split(split);
                    return Arrays.asList(values);
                }
                case OBJECT: {
                    if (val instanceof Map) {
                        return val;
                    }
                    return JsonUtil.toMap(val.toString(), true);
                }
                case BOOLEAN: {
                    if (val instanceof Boolean) {
                        return val;
                    }
                    return ESSyncUtil.castToBoolean(val);
                }
                case STATIC_METHOD: {
                    if (staticMethodAccessor == null) {
                        staticMethodAccessor = new StaticMethodAccessor<>(method, ESStaticMethodParam.class);
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
                    return staticMethodAccessor.apply(new ESStaticMethodParam(val, mapping, fieldName, parentFieldName));
                }
                case ARRAY_SQL:
                case OBJECT_SQL:
                default: {
                    return val;
                }
            }
        }

        public String getMethod() {
            return method;
        }

        public void setMethod(String method) {
            this.method = method;
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
            if (type != null && !type.isSqlType()) {
                return null;
            }
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

        public String sql() {
            return SqlParser.removeGroupBy(sql);
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

        public boolean isSqlType() {
            return type != null && type.isSqlType();
        }

        public enum Type {
            /**
             * 数组(逗号分割), 对象(JSON.parse), 数组(sql多条查询), 对象(sql单条查询) ,boolean
             */
            ARRAY, OBJECT, ARRAY_SQL, OBJECT_SQL, BOOLEAN, STATIC_METHOD, URL;

            public boolean isSqlType() {
                return this == Type.OBJECT_SQL || this == Type.ARRAY_SQL;
            }

            public boolean isSingleJoinType() {
                return this == Type.OBJECT_SQL;
            }
        }
    }
}
