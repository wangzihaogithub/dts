package com.github.dts.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

    public List<SchemaItem.TableItem> getNestedSlaveTableList(String tableName) {
        List<SchemaItem.TableItem> list = new ArrayList<>();
        for (SchemaItem.TableItem tableItem : nestedSlaveTableList) {
            if (tableName.equalsIgnoreCase(tableItem.getTableName())) {
                list.add(tableItem);
            }
        }
        return list;
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
        return "Dependent{" +
                "dml=" + dml +
                ", schemaItem=" + schemaItem +
                ", index=" + index +
                '}';
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

    public boolean isMainTable() {
        return isIndexMainTable() || isNestedMainTable();
    }

    public boolean isNestedSlaveTable() {
        for (SchemaItem.TableItem tableItem : nestedSlaveTableList) {
            if (dml.getTable().equalsIgnoreCase(tableItem.getTableName())) {
                return true;
            }
        }
        return false;
    }
}