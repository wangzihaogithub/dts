package com.github.dts.util;

import java.util.*;

/**
 * 表依赖关系
 */
public class SqlDependent {
    private final Dml dml;
    private final SchemaItem schemaItem;
    private final int index;
    private transient String dmlKey;
    private Boolean effect;

    public SqlDependent(SchemaItem schemaItem, int index,
                        Dml dml) {
        this(schemaItem, index, dml, null);
    }

    public SqlDependent(SchemaItem schemaItem, int index,
                        Dml dml, Boolean effect) {
        this.schemaItem = schemaItem;
        this.index = index;
        this.dml = dml;
        this.effect = effect;
    }

    public boolean isJoinByMainTablePrimaryKey() {
        return isIndexMainTable() || schemaItem.isJoinByMainTablePrimaryKey();
    }

    public boolean isJoinBySlaveTableForeignKey() {
        return !isJoinByMainTablePrimaryKey();
    }

    public List<SchemaItem.TableItem> getNestedSlaveTableList(String tableName) {
        List<SchemaItem.TableItem> list = new ArrayList<>();
        for (SchemaItem.TableItem tableItem : getNestedSlaveTableList()) {
            if (tableName.equalsIgnoreCase(tableItem.getTableName())) {
                list.add(tableItem);
            }
        }
        return list;
    }

    public Map<String, Object> getDataMap() {
        if (!ESSyncUtil.isEmpty(dml.getData()) && index < dml.getData().size()) {
            return dml.getData().get(index);
        } else {
            return null;
        }
    }

    public Map<String, Object> getOldMap() {
        if (!ESSyncUtil.isEmpty(dml.getOld()) && index < dml.getOld().size()) {
            return dml.getOld().get(index);
        } else {
            return null;
        }
    }

    public Map<String, Object> getMergeAfterDataMap() {
        Map<String, Object> mergeDataMap = new HashMap<>();
        Map<String, Object> oldMap = getOldMap();
        if (oldMap != null) {
            mergeDataMap.putAll(oldMap);
        }
        Map<String, Object> dataMap = getDataMap();
        if (dataMap != null) {
            mergeDataMap.putAll(dataMap);
        }
        return mergeDataMap;
    }

    public Map<String, Object> getMergeBeforeDataMap() {
        Map<String, Object> mergeDataMap = new HashMap<>();
        Map<String, Object> dataMap = getDataMap();
        if (dataMap != null) {
            mergeDataMap.putAll(dataMap);
        }
        Map<String, Object> oldMap = getOldMap();
        if (oldMap != null) {
            mergeDataMap.putAll(oldMap);
        }
        return mergeDataMap;
    }

    public Dml getDml() {
        return dml;
    }

    public SchemaItem getSchemaItem() {
        return schemaItem;
    }

    public int getIndex() {
        return index;
    }

    /**
     * 索引主表
     *
     * @return 索引主表
     */
    public SchemaItem.TableItem getIndexMainTable() {
        return schemaItem.getObjectField().getEsMapping().getSchemaItem().getMainTable();
    }

    /**
     * 嵌套文档主表
     *
     * @return 嵌套文档主表
     */
    public SchemaItem.TableItem getNestedMainTable() {
        return schemaItem.getObjectField().getParamSql().getSchemaItem().getMainTable();
    }

    /**
     * 嵌套文档从表
     *
     * @return 嵌套文档从表
     */
    public List<SchemaItem.TableItem> getNestedSlaveTableList() {
        return schemaItem.getObjectField().getParamSql().getSchemaItem().getSlaveTableList();
    }

    /**
     * DML是否影响了文档
     *
     * @return true
     */
    public boolean isEffect() {
        if (effect == null) {
            boolean effect;
            if (dml.isTypeInit()) {
                effect = true;
            } else if (dml.isTypeUpdate()) {
                Map<String, Object> oldMap = getOldMap();
                // 查找是否存在依赖
                effect = oldMap != null && !oldMap.isEmpty() && schemaItem.existTableColumn(dml.getTable(), oldMap.keySet());
            } else if (dml.isTypeInsert()) {
                // 查找是否存在依赖
                effect = schemaItem.existTableColumn(dml.getTable(), null);
            } else if (dml.isTypeDelete()) {
                // 查找是否存在依赖
                effect = schemaItem.existTableColumn(dml.getTable(), null);
            } else {
                effect = false;
            }
            this.effect = effect;
        }
        return effect;
    }

    public void setEffect(Boolean effect) {
        this.effect = effect;
    }

    @Override
    public String toString() {
        List<Map<String, Object>> oldList = dml.getOld();
        List<Map<String, Object>> dataList = dml.getData();
        Map<String, Object> old = oldList != null ? oldList.get(index) : null;
        Map<String, Object> data = dataList != null ? dataList.get(index) : null;
        String oldString = old == null ? "" : old.keySet().toString();
        Collection<String> pkNames = dml.getPkNames();
        StringJoiner pkJoiner = null;
        if (pkNames != null && !pkNames.isEmpty()) {
            pkJoiner = new StringJoiner(",");
            for (String pkName : pkNames) {
                Object pkValue = old != null ? old.get(pkName) : null;
                if (pkValue == null && data != null) {
                    pkValue = data.get(pkName);
                }
                pkJoiner.add(String.valueOf(pkValue));
            }
        }
        return dml.getType() + "#" + (pkJoiner == null ? "" : pkJoiner) +
                "{" + dml.getTable() + oldString + " -> " + schemaItem + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SqlDependent)) return false;
        SqlDependent sqlDependent = (SqlDependent) o;
        return index == sqlDependent.index && Objects.equals(dml, sqlDependent.dml) && Objects.equals(schemaItem, sqlDependent.schemaItem);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dml, schemaItem, index);
    }

    public String dmlKey() {
        if (dmlKey == null) {
            List<Map<String, Object>> oldList = dml.getOld();
            Map<String, Object> old = oldList != null ? oldList.get(index) : null;
            dmlKey = dml.getType() + "_" + dml.getTable() + "_" + (old == null ? "null" : old.keySet());
        }
        return dmlKey;
    }

    public boolean isIndexMainTable() {
        return schemaItem.isIndexMainTable(dml.getTable());
    }

    public boolean isNestedMainTable() {
        return dml.getTable().equalsIgnoreCase(getNestedMainTable().getTableName());
    }

    public boolean isNestedSlaveTable() {
        for (SchemaItem.TableItem tableItem : getNestedSlaveTableList()) {
            if (dml.getTable().equalsIgnoreCase(tableItem.getTableName())) {
                return true;
            }
        }
        return false;
    }

    public boolean containsObjectField(Collection<String> fieldName) {
        ESSyncConfig.ObjectField objectField = schemaItem.getObjectField();
        if (objectField == null) {
            return false;
        }
        return fieldName.contains(objectField.getFieldName());
    }
}