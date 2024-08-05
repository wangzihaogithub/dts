package com.github.dts.util;

import java.util.*;

/**
 * 表依赖关系
 */
public class Dependent {
    private final Dml dml;
    private final SchemaItem schemaItem;
    private final int index;
    /**
     * 索引主表
     */
    private final SchemaItem.TableItem indexMainTable;
    /**
     * 嵌套文档主表
     */
    private final SchemaItem.TableItem nestedMainTable;
    /**
     * 嵌套文档从表
     */
    private final List<SchemaItem.TableItem> nestedSlaveTableList;

    public Dependent(SchemaItem schemaItem, int index,
                     SchemaItem.TableItem indexMainTable,
                     SchemaItem.TableItem nestedMainTable,
                     List<SchemaItem.TableItem> nestedSlaveTableList,
                     Dml dml) {
        this.schemaItem = schemaItem;
        this.index = index;
        this.indexMainTable = indexMainTable;
        this.nestedSlaveTableList = nestedSlaveTableList;
        this.nestedMainTable = nestedMainTable;
        this.dml = dml;
    }

    public boolean isJoinByParentSlaveTableForeignKey() {
        if (isIndexMainTable()) {
            return false;
        } else {
            return !schemaItem.isJoinByParentPrimaryKey();
        }
    }

    public List<SchemaItem.TableItem> getNestedSlaveTableList(String tableName) {
        List<SchemaItem.TableItem> list = new ArrayList<>();
        for (SchemaItem.TableItem tableItem : nestedSlaveTableList) {
            if (tableName.equalsIgnoreCase(tableItem.getTableName())) {
                list.add(tableItem);
            }
        }
        return list;
    }

    public Map<String, Object> getMergeDataMap() {
        Map<String, Object> mergeDataMap = new HashMap<>();
        if (!ESSyncUtil.isEmpty(dml.getOld()) && index < dml.getOld().size()) {
            mergeDataMap.putAll(dml.getOld().get(index));
        }
        if (!ESSyncUtil.isEmpty(dml.getData()) && index < dml.getData().size()) {
            mergeDataMap.putAll(dml.getData().get(index));
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

    public SchemaItem.TableItem getIndexMainTable() {
        return indexMainTable;
    }

    public SchemaItem.TableItem getNestedMainTable() {
        return nestedMainTable;
    }

    public List<SchemaItem.TableItem> getNestedSlaveTableList() {
        return nestedSlaveTableList;
    }

    @Override
    public String toString() {
        return dml.getType() + "." + dml.getTable() + "(" + schemaItem + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Dependent)) return false;
        Dependent dependent = (Dependent) o;
        return index == dependent.index && Objects.equals(dml, dependent.dml) && Objects.equals(schemaItem, dependent.schemaItem);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dml, schemaItem, index);
    }

    public boolean isIndexMainTable() {
        return dml.getTable().equalsIgnoreCase(indexMainTable.getTableName());
    }

    public boolean isNestedMainTable() {
        return dml.getTable().equalsIgnoreCase(nestedMainTable.getTableName());
    }

    public boolean isNestedSlaveTable() {
        for (SchemaItem.TableItem tableItem : nestedSlaveTableList) {
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