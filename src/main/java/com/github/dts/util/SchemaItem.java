package com.github.dts.util;

import com.github.dts.impl.elasticsearch.nested.SQL;
import com.github.dts.util.ESSyncConfig.ESMapping;

import java.util.*;
import java.util.stream.Collectors;

/**
 * ES 映射配置视图
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class SchemaItem {
    private final boolean check;
    private Map<String, TableItem> aliasTableItems = Util.newLinkedCaseInsensitiveMap(); // 别名对应表名
    private Map<String, FieldItem> selectFields = Util.newLinkedCaseInsensitiveMap();  // 查询字段
    private String sql;
    private volatile Map<String, FieldItem> fields = Util.newLinkedCaseInsensitiveMap();
    private volatile Map<String, List<TableItem>> tableItemAliases;
    private volatile Map<String, List<FieldItem>> columnFields;
    private volatile Boolean allFieldsSimple;
    private ESSyncConfig.ObjectField objectField;
    private ESMapping esMapping;
    private TableItem mainTable;
    private List<TableItem> slaveTableList;
    private FieldItem idField;
    private Set<ColumnItem> groupByIdColumns;
    private Boolean joinByMainTablePrimaryKey;
    private Map<String, List<String>> onSlaveTableChangeWhereSqlColumnList;
    private Map<String, List<String>> onMainTableChangeWhereSqlColumnList;
    private Set<String> onSlaveTableChangeWhereSqlVarList;
    private Set<String> onMainTableChangeWhereSqlVarList;
    /**
     * 表的别名和字段的关系 《表的别名，List《字段》》，如果别名为空表示用户没写别名，默认为主表
     */
    private Map<String, List<String>> onSlaveTableChangeOrderBySqlColumnList;
    /**
     * 表的别名和字段的关系 《表的别名，List《字段》》，如果别名为空表示用户没写别名，默认为主表
     */
    private Map<String, List<String>> onMainTableChangeOrderBySqlColumnList;

    public SchemaItem(boolean check) {
        this.check = check;
    }

    public static Object getColumnValue(Map<String, Object> data, String columnName) {
        return data.get(columnName);
    }

    public static void main(String[] args) {
        Map<String, List<String>> columnList = SqlParser.getColumnList("WHERE corpRelationTag.corp_id = #{corp_id} ");

    }

    private static boolean matchColumnItem(ColumnItem columnItem, String tableName, Map<String, TableItem> aliasTableItems, Set<String> fieldNameSet) {
        TableItem tableItem = aliasTableItems.get(columnItem.getOwner());
        if (tableItem == null || !tableName.equalsIgnoreCase(tableItem.getTableName())) {
            return false;
        }
        return fieldNameSet == null || fieldNameSet.contains(columnItem.getColumnName());
    }

    public List<FieldItem> selectField(SchemaItem.FieldItem currFieldItem) {
        List<FieldItem> list = new ArrayList<>(2);
        if (currFieldItem != null) {
            for (SchemaItem.FieldItem fieldItem : selectFields.values()) {
                if (fieldItem == currFieldItem || fieldItem.equalsColumn(currFieldItem)) {
                    list.add(fieldItem);
                }
            }
        }
        return list;
    }

    public String getDesc() {
        if (objectField != null) {
            return "nested[" + objectField.getFieldName() + "]";
        } else {
            return "doc[" + String.join(",", tableItemAliases.keySet()) + "]";
        }
    }

    public void init(ESSyncConfig.ObjectField objectField, ESMapping esMapping) {
        this.objectField = objectField;
        this.esMapping = esMapping;
        this.getTableItemAliases();
        this.getColumnFields();
        this.isAllFieldsSimple();
        aliasTableItems.values().forEach(tableItem -> {
            tableItem.getRelationTableFields();
            tableItem.getRelationSelectFieldItems();
        });
        TableItem mainTable = getMainTable();
        if (mainTable != null) {
            mainTable.main = true;
        }
        getSlaveTableList();
        isJoinByMainTablePrimaryKey();
        getGroupByIdColumns();
        getIdField();
        getOnMainTableChangeWhereSqlColumnList();
        getOnSlaveTableChangeWhereSqlColumnList();
        getOnMainTableChangeWhereSqlVarList();
        getOnSlaveTableChangeWhereSqlVarList();
        getOnMainTableChangeOrderBySqlColumnList();
        getOnSlaveTableChangeOrderBySqlColumnList();
    }

    public boolean isIndexMainTable(String tableName) {
        TableItem mainTable = esMapping.getSchemaItem().getMainTable();
        return tableName.equalsIgnoreCase(mainTable.getTableName());
    }

    public boolean isJoinByMainTablePrimaryKey() {
        if (joinByMainTablePrimaryKey == null) {
            if (objectField != null && objectField.isSqlType()) {
                List<SqlParser.BinaryOpExpr> varColumnList = SqlParser.getVarColumnList(getObjectField().getParamSql().getOnMainTableChangeWhereSql());
                FieldItem idField = getIdField();
                String id = SQL.wrapPlaceholder(idField.getColumnName());
                joinByMainTablePrimaryKey = varColumnList.stream().anyMatch(e -> e.getValue().equalsIgnoreCase(id));
            } else {
                joinByMainTablePrimaryKey = false;
            }
        }
        return joinByMainTablePrimaryKey;
    }

    public Set<ColumnItem> getGroupByIdColumns() {
        if (groupByIdColumns == null) {
            Set<ColumnItem> groupByIdColumns;
            ESSyncConfig.ObjectField.ParamSql paramSql;
            if (objectField != null && (paramSql = objectField.getParamSql()) != null) {
                String[] groupBy = paramSql.groupByIdColumns();
                if (groupBy != null && groupBy.length > 0) {
                    groupByIdColumns = Arrays.stream(groupBy).map(ColumnItem::parse).collect(Collectors.toCollection(LinkedHashSet::new));
                } else {
                    if (objectField.getType().isSingleJoinType()) {
                        groupByIdColumns = parseByObjectFieldIdColumns();
                    } else if (!isJoinByMainTablePrimaryKey() && objectField.getType().isArraySqlType()) {
                        throw new IllegalArgumentException("the join sql must have group by. sql = " + sql);
                    } else {
                        groupByIdColumns = Collections.emptySet();
                    }
                }
            } else {
                groupByIdColumns = parseByMainIdColumns();
            }
            this.groupByIdColumns = groupByIdColumns;
        }
        return groupByIdColumns;
    }

    public List<ColumnItem> getColumnItemListByColumn(String column) {
        List<ColumnItem> list = new ArrayList<>();
        for (FieldItem fieldItem : selectFields.values()) {
            Collection<ColumnItem> columnItems = fieldItem.getColumnItems();
            if (columnItems.size() != 1) {
                continue;
            }
            ColumnItem columnItem = columnItems.iterator().next();
            if (columnItem.getColumnName().equalsIgnoreCase(column)) {
                list.add(columnItem);
            }
        }
        return list;
    }

    public Set<String> getOnMainTableChangeWhereSqlVarList() {
        if (onMainTableChangeWhereSqlVarList == null && objectField != null && objectField.isSqlType()) {
            this.onMainTableChangeWhereSqlVarList = Util.newLinkedCaseInsensitiveSet(SQL.convertToSql(objectField.getParamSql().getOnMainTableChangeWhereSql(), Collections.emptyMap()).getArgsMap().keySet());
        }
        return onMainTableChangeWhereSqlVarList;
    }

    public Set<String> getOnSlaveTableChangeWhereSqlVarList() {
        if (onSlaveTableChangeWhereSqlVarList == null && objectField != null && objectField.isSqlType()) {
            this.onSlaveTableChangeWhereSqlVarList = Util.newLinkedCaseInsensitiveSet(SQL.convertToSql(objectField.getParamSql().getOnSlaveTableChangeWhereSql(), Collections.emptyMap()).getArgsMap().keySet());
        }
        return onSlaveTableChangeWhereSqlVarList;
    }

    public boolean existAnyOrderColumn() {
        return !getOnSlaveTableChangeOrderBySqlColumnList().isEmpty()
                || !getOnMainTableChangeOrderBySqlColumnList().isEmpty();
    }

    public Map<String, List<String>> getOnMainTableChangeOrderBySqlColumnList() {
        if (onMainTableChangeOrderBySqlColumnList == null && objectField != null && objectField.isSqlType()) {
            this.onMainTableChangeOrderBySqlColumnList = SqlParser.getOrderByColumnList(objectField.getParamSql().getOnMainTableChangeWhereSql());
        }
        return onMainTableChangeOrderBySqlColumnList;
    }

    public Map<String, List<String>> getOnSlaveTableChangeOrderBySqlColumnList() {
        if (onSlaveTableChangeOrderBySqlColumnList == null && objectField != null && objectField.isSqlType()) {
            this.onSlaveTableChangeOrderBySqlColumnList = SqlParser.getOrderByColumnList(objectField.getParamSql().getOnSlaveTableChangeWhereSql());
        }
        return onSlaveTableChangeOrderBySqlColumnList;
    }

    public Map<String, List<String>> getOnMainTableChangeWhereSqlColumnList() {
        if (onMainTableChangeWhereSqlColumnList == null && objectField != null && objectField.isSqlType()) {
            this.onMainTableChangeWhereSqlColumnList = SqlParser.getColumnList(objectField.getParamSql().getOnMainTableChangeWhereSql());
        }
        return onMainTableChangeWhereSqlColumnList;
    }

    public Map<String, List<String>> getOnSlaveTableChangeWhereSqlColumnList() {
        if (onSlaveTableChangeWhereSqlColumnList == null && objectField != null && objectField.isSqlType()) {
            this.onSlaveTableChangeWhereSqlColumnList = SqlParser.getColumnList(objectField.getParamSql().getOnSlaveTableChangeWhereSql());
        }
        return onSlaveTableChangeWhereSqlColumnList;
    }

    private Set<ColumnItem> parseByObjectFieldIdColumns() {
        Set<ColumnItem> idColumns = new LinkedHashSet<>();
        TableItem mainTable = getMainTable();
        List<String> mainColumnList = SqlParser.getVarColumnList(objectField.getParamSql().getOnSlaveTableChangeWhereSql()).stream()
                .filter(e -> e.isOwner(mainTable.getAlias()))
                .map(SqlParser.BinaryOpExpr::getName)
                .collect(Collectors.toList());
        if (mainColumnList.isEmpty()) {
            mainColumnList = SqlParser.getVarColumnList(objectField.getParamSql().getOnMainTableChangeWhereSql()).stream()
                    .filter(e -> e.isOwner(mainTable.getAlias()))
                    .map(SqlParser.BinaryOpExpr::getName)
                    .collect(Collectors.toList());
        }
        LinkedHashSet<String> mainColumnSet = new LinkedHashSet<>(mainColumnList);
        for (String column : mainColumnSet) {
            ColumnItem columnItem = new ColumnItem();
            columnItem.setColumnName(column);
            columnItem.setOwner(mainTable.getAlias());
            idColumns.add(columnItem);
        }
        return idColumns;
    }

    private Set<ColumnItem> parseByMainIdColumns() {
        Set<ColumnItem> idColumns = new LinkedHashSet<>();
        TableItem mainTable = getMainTable();
        for (ColumnItem idColumnItem : getIdFieldItem(esMapping).getColumnItems()) {
            if ((mainTable.getAlias() == null && idColumnItem.getOwner() == null)
                    || (mainTable.getAlias() != null && mainTable.getAlias().equals(idColumnItem.getOwner()))) {
                idColumns.add(idColumnItem);
            }
        }
        if (idColumns.isEmpty()) {
            throw new RuntimeException("Not found primary key field in main table");
        }
        return idColumns;
    }

    @Override
    public String toString() {
        if (esMapping == null) {
            return sql;
        }
        if (objectField == null) {
            return String.valueOf(esMapping);
        } else {
            return Objects.toString(esMapping, "") + " [" + objectField + "]";
        }
    }

    public FieldItem getIdField() {
        if (idField == null) {
            SchemaItem schemaItem = esMapping.getSchemaItem();
            String idFieldName = esMapping.get_id() == null ? esMapping.getPk() : esMapping.get_id();
            for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
                if (fieldItem.equalsField(idFieldName)) {
                    idField = fieldItem;
                    break;
                }
            }
        }
        return idField;
    }

    public ESMapping getEsMapping() {
        return esMapping;
    }

    public ESSyncConfig.ObjectField getObjectField() {
        return objectField;
    }

    public Map<String, TableItem> getAliasTableItems() {
        return aliasTableItems;
    }

    public void setAliasTableItems(Map<String, TableItem> aliasTableItems) {
        this.aliasTableItems = aliasTableItems;
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

    public Map<String, FieldItem> getSelectFields() {
        return selectFields;
    }

    public void setSelectFields(Map<String, FieldItem> selectFields) {
        this.selectFields = selectFields;
    }

    public boolean existTable(String tableName) {
        if (objectField != null && objectField.isSqlType()) {
            TableItem mainTable = objectField.getParamSql().getSchemaItem().getMainTable();
            if (tableName.equalsIgnoreCase(mainTable.getTableName())) {
                return true;
            } else if (isIndexMainTable(tableName)) {
                return true;
            }
        }
        return getTableItemAliases().containsKey(tableName);
    }

    /**
     * 查找是否存在依赖
     *
     * @param tableName   查找的表名
     * @param columnNames 查找的字段集合， 如果是insert，delete，则columnNames为null
     * @return true=存在依赖，false=无依赖
     */
    public boolean existTableColumn(String tableName, Collection<String> columnNames) {
        // HashSet 快速搜索
        Set<String> columnNameSet = columnNames == null ? null : Util.newLinkedCaseInsensitiveSet(columnNames);
        // 查找是否存在依赖select的字段
        for (FieldItem fieldItem : fields.values()) {
            for (ColumnItem columnItem : fieldItem.getColumnItems()) {
                if (matchColumnItem(columnItem, tableName, aliasTableItems, columnNameSet)) {
                    return true;
                }
            }
        }
        // 查找是否存在依赖group的字段
        for (ColumnItem groupByIdColumn : groupByIdColumns) {
            if (matchColumnItem(groupByIdColumn, tableName, aliasTableItems, columnNameSet)) {
                return true;
            }
        }
        //
        if (objectField != null && objectField.isSqlType()) {
            TableItem mainTable = objectField.getParamSql().getSchemaItem().getMainTable();
            if (tableName.equalsIgnoreCase(mainTable.getTableName())) {
                // 查找是否存在依赖where的字段
                if (columnNameSet == null || existColumn(columnNameSet, getOnSlaveTableChangeWhereSqlVarList())) {
                    return true;
                }
                // 查找是否存在依赖orderBy的字段
                if (existOrderByColumn(columnNameSet, tableName, getOnSlaveTableChangeOrderBySqlColumnList())) {
                    return true;
                }
            } else if (isIndexMainTable(tableName)) {
                // 查找是否存在依赖where的字段
                if (columnNameSet == null || existColumn(columnNameSet, getOnMainTableChangeWhereSqlVarList())) {
                    return true;
                }
                // 查找是否存在依赖orderBy的字段
                if (existOrderByColumn(columnNameSet, tableName, getOnMainTableChangeOrderBySqlColumnList())) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean existColumn(Set<String> columnNameSet, Set<String> varList) {
        for (String s : varList) {
            if (columnNameSet.contains(s)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 根据表名查字段
     *
     * @param tableName          表名
     * @param aliasColumnListMap 表的别名和字段的关系 《表的别名，List《字段》》，如果别名为空表示用户没写别名，默认为主表
     * @return 这个表名的所有依赖字段
     */
    private List<String> getColumnByTableName(String tableName, Map<String, List<String>> aliasColumnListMap) {
        List<String> result = new ArrayList<>(aliasColumnListMap.size());
        for (Map.Entry<String, List<String>> entry : aliasColumnListMap.entrySet()) {
            String alias = entry.getKey();
            TableItem tableItem = aliasTableItems.get(alias);
            // 如果别名为空表示用户没写别名，默认为主表
            if (tableItem == null && isIndexMainTable(tableName)) {
                result.addAll(entry.getValue());
            } else if (tableItem != null && tableItem.getTableName().equalsIgnoreCase(tableName)) {
                // 表的别名
                result.addAll(entry.getValue());
            }
        }
        return result;
    }

    /**
     * 查找是否存在依赖orderBy的字段
     *
     * @param columnNameSet      查找的字段集合
     * @param tableName          查找的表名
     * @param aliasColumnListMap 被查找的表的别名和字段的关系 《表的别名，List《字段》》，如果别名为空表示用户没写别名，默认为主表
     * @return true=存在
     */
    private boolean existOrderByColumn(Set<String> columnNameSet, String tableName, Map<String, List<String>> aliasColumnListMap) {
        Collection<String> columnList = getColumnByTableName(tableName, aliasColumnListMap);
        for (String column : columnList) {
            if (columnNameSet.contains(column)) {
                return true;
            }
        }
        return false;
    }

    public Map<String, FieldItem> getFields() {
        return fields;
    }

    public void setFields(Map<String, FieldItem> fields) {
        this.fields = fields;
    }

    public Map<String, List<TableItem>> getTableItemAliases() {
        if (tableItemAliases == null) {
            synchronized (SchemaItem.class) {
                if (tableItemAliases == null) {
                    tableItemAliases = Util.newLinkedCaseInsensitiveMap();
                    aliasTableItems.forEach((alias, tableItem) -> {
                        List<TableItem> aliases = tableItemAliases
                                .computeIfAbsent(tableItem.getTableName(), k -> new ArrayList<>());
                        aliases.add(tableItem);
                    });
                }
            }
        }
        return tableItemAliases;
    }

    public List<String> getTableItemAliases(String tableName) {
        List<TableItem> list = getTableItemAliases().get(tableName);
        if (list == null || list.isEmpty()) {
            return Collections.emptyList();
        }
        return list.stream()
                .map(TableItem::getAlias)
                .collect(Collectors.toList());
    }

    public Map<String, List<FieldItem>> getColumnFields() {
        if (columnFields == null) {
            synchronized (SchemaItem.class) {
                if (columnFields == null) {
                    columnFields = Util.newLinkedCaseInsensitiveMap();
                    getSelectFields()
                            .forEach((fieldName, fieldItem) -> fieldItem.getColumnItems().forEach(columnItem -> {
                                // TableItem tableItem = getAliasTableItems().get(columnItem.getOwner());
                                // if (!tableItem.isSubQuery()) {
                                List<FieldItem> fieldItems = columnFields.computeIfAbsent(
                                        columnItem.getOwner() + "." + columnItem.getColumnName(),
                                        k -> new ArrayList<>());
                                fieldItems.add(fieldItem);
                                // } else {
                                // tableItem.getSubQueryFields().forEach(subQueryField -> {
                                // List<FieldItem> fieldItems = columnFields.computeIfAbsent(
                                // columnItem.getOwner() + "." + subQueryField.getColumn().getColumnName(),
                                // k -> new ArrayList<>());
                                // fieldItems.add(fieldItem);
                                // });
                                // }
                            }));
                }
            }
        }
        return columnFields;
    }

    public boolean isAllFieldsSimple() {
        if (allFieldsSimple == null) {
            synchronized (SchemaItem.class) {
                if (allFieldsSimple == null) {
                    boolean allFieldsSimple = true;
                    for (FieldItem fieldItem : getSelectFields().values()) {
                        if (fieldItem.isMethod() || fieldItem.isBinaryOp() || fieldItem.getColumnItems().size() != 1) {
                            allFieldsSimple = false;
                            break;
                        }
                    }
                    this.allFieldsSimple = allFieldsSimple;
                }
            }
        }

        return allFieldsSimple;
    }

    public TableItem getMainTable() {
        if (mainTable == null) {
            if (aliasTableItems.size() == 1) {
                mainTable = aliasTableItems.values().iterator().next();
            } else {
                ESSyncConfig.ObjectField.ParamSql paramSql;
                if (objectField != null && (paramSql = objectField.getParamSql()) != null) {
                    Map<String, List<String>> childColumnList = SqlParser.getColumnList(paramSql.getOnSlaveTableChangeWhereSql());
                    String owner;
                    if (childColumnList.isEmpty()) {
                        Map<String, List<String>> parentColumnList = SqlParser.getColumnList(paramSql.getOnMainTableChangeWhereSql());
                        if (parentColumnList.isEmpty()) {
                            owner = "";
                        } else {
                            owner = parentColumnList.keySet().iterator().next();
                        }
                    } else {
                        owner = childColumnList.keySet().iterator().next();
                    }
                    mainTable = aliasTableItems.get(owner);
                } else if (esMapping != null) {
                    SchemaItem schemaItem = esMapping.getSchemaItem();
                    FieldItem pkFieldItem = schemaItem.getSelectFields().get(esMapping.getPk());
                    if (pkFieldItem == null) {
                        pkFieldItem = schemaItem.getSelectFields().get(esMapping.get_id());
                    }
                    String owner = Objects.toString(pkFieldItem.getOwner(), "");
                    mainTable = aliasTableItems.get(owner);
                } else {
                    mainTable = aliasTableItems.values().iterator().next();
                }
            }
        }
        return mainTable;
    }

    public List<TableItem> getSlaveTableList() {
        if (slaveTableList == null) {
            if (aliasTableItems.size() > 1) {
                List<TableItem> list = new ArrayList<>(aliasTableItems.size() - 1);
                TableItem mainTable = getMainTable();
                for (TableItem tableItem : aliasTableItems.values()) {
                    if (mainTable != tableItem) {
                        list.add(tableItem);
                    }
                }
                slaveTableList = list;
            } else {
                slaveTableList = Collections.emptyList();
            }
        }
        return slaveTableList;
    }

    public FieldItem getIdFieldItem(ESMapping mapping) {
        if (mapping.get_id() != null) {
            return getSelectFields().get(mapping.get_id());
        } else {
            return getSelectFields().get(mapping.getPk());
        }
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public static class TableItem {

        private SchemaItem schemaItem;

        private String schema;
        private String tableName;
        private String alias;
        private String subQuerySql;
        private List<FieldItem> subQueryFields = new ArrayList<>();
        private List<RelationFieldsPair> relationFields = new ArrayList<>();

        private boolean main;
        private boolean subQuery;

        private volatile Map<FieldItem, List<FieldItem>> relationTableFields;               // 当前表关联条件字段对应主表查询字段
        private volatile List<FieldItem> relationSelectFieldItems;          // 子表所在主表的查询字段

        public TableItem(SchemaItem schemaItem) {
            this.schemaItem = schemaItem;
        }

        @Override
        public String toString() {
            return Objects.toString(alias, "") + "." + tableName + " " + relationTableFields.keySet();
        }

        public SchemaItem getSchemaItem() {
            return schemaItem;
        }

        public void setSchemaItem(SchemaItem schemaItem) {
            this.schemaItem = schemaItem;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public String getSubQuerySql() {
            return subQuerySql;
        }

        public void setSubQuerySql(String subQuerySql) {
            this.subQuerySql = subQuerySql;
        }

        public boolean isMain() {
            return main;
        }

        public boolean isSubQuery() {
            return subQuery;
        }

        public void setSubQuery(boolean subQuery) {
            this.subQuery = subQuery;
        }

        public List<FieldItem> getSubQueryFields() {
            return subQueryFields;
        }

        public void setSubQueryFields(List<FieldItem> subQueryFields) {
            this.subQueryFields = subQueryFields;
        }

        public boolean containsOwnerColumn(List<String> columnNameList) {
            if (columnNameList == null || columnNameList.isEmpty()) {
                return false;
            }
            for (String columnName : columnNameList) {
                if (!containsOwnerColumn(columnName)) {
                    return false;
                }
            }
            return true;
        }

        private boolean containsOwnerColumn(String columnName) {
            for (RelationFieldsPair relationField : relationFields) {
                FieldItem leftFieldItem = relationField.getLeftFieldItem();
                FieldItem rightFieldItem = relationField.getRightFieldItem();

                if ((leftFieldItem != null && leftFieldItem.containsOwnerColumn(alias, columnName))
                        || (rightFieldItem != null && rightFieldItem.containsOwnerColumn(alias, columnName))
                ) {
                    return true;
                }
            }
            return false;
        }

        public List<RelationFieldsPair> getRelationFields() {
            return relationFields;
        }

        public void setRelationFields(List<RelationFieldsPair> relationFields) {
            this.relationFields = relationFields;
        }

        public Map<FieldItem, List<FieldItem>> getRelationTableFields() {
            if (relationTableFields == null) {
                synchronized (SchemaItem.class) {
                    if (relationTableFields == null) {
                        relationTableFields = new LinkedHashMap<>();
                        for (RelationFieldsPair relationFieldsPair : getRelationFields()) {
                            FieldItem leftFieldItem = relationFieldsPair.getLeftFieldItem();
                            FieldItem rightFieldItem = relationFieldsPair.getRightFieldItem();
                            FieldItem currentTableRelField = null;
                            if (leftFieldItem != null && getAlias().equals(leftFieldItem.getOwner())) {
                                currentTableRelField = leftFieldItem;
                            } else if (rightFieldItem != null && getAlias().equals(rightFieldItem.getOwner())) {
                                currentTableRelField = rightFieldItem;
                            }

                            if (currentTableRelField != null) {
                                int putCount = 0;
                                if (leftFieldItem != null) {
                                    for (ColumnItem columnItem : leftFieldItem.getColumnItems()) {
                                        List<FieldItem> selectFieldItem = getSchemaItem().getColumnFields()
                                                .get(columnItem.getOwner() + "." + columnItem.getColumnName());
                                        if (selectFieldItem != null && !selectFieldItem.isEmpty()) {
                                            relationTableFields.put(currentTableRelField, selectFieldItem);
                                            putCount++;
                                        }
                                    }
                                }
                                if (rightFieldItem != null) {
                                    for (ColumnItem columnItem : rightFieldItem.getColumnItems()) {
                                        List<FieldItem> selectFieldItem = getSchemaItem().getColumnFields()
                                                .get(columnItem.getOwner() + "." + columnItem.getColumnName());
                                        if (selectFieldItem != null && !selectFieldItem.isEmpty()) {
                                            relationTableFields.put(currentTableRelField, selectFieldItem);
                                            putCount++;
                                        }
                                    }
                                }
                                if (putCount == 0 && schemaItem.check) {
                                    throw new UnsupportedOperationException(
                                            tableName + "Relation condition column must in select columns." + relationFieldsPair);
                                }
                            }
                        }
                    }
                }
            }
            return relationTableFields;
        }

        public List<FieldItem> getRelationSelectFieldItems() {
            if (relationSelectFieldItems == null) {
                synchronized (SchemaItem.class) {
                    if (relationSelectFieldItems == null) {
                        List<FieldItem> relationSelectFieldItemsTmp = new ArrayList<>();
                        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
                            if (fieldItem.getOwners().contains(getAlias())) {
                                relationSelectFieldItemsTmp.add(fieldItem);
                            }
                        }
                        relationSelectFieldItems = relationSelectFieldItemsTmp;
                    }
                }
            }
            return relationSelectFieldItems;
        }
    }

    public static class RelationFieldsPair {

        private FieldItem leftFieldItem;
        private FieldItem rightFieldItem;
        private Object leftValue;
        private Object rightValue;

        public RelationFieldsPair(FieldItem leftFieldItem, Object leftValue, FieldItem rightFieldItem, Object rightValue) {
            this.leftFieldItem = leftFieldItem;
            this.rightFieldItem = rightFieldItem;
            this.leftValue = leftValue;
            this.rightValue = rightValue;
        }

        @Override
        public String toString() {
            return "Pair{" +
                    "left=" + leftFieldItem +
                    ", right=" + rightFieldItem +
                    '}';
        }

        public FieldItem getLeftFieldItem() {
            return leftFieldItem;
        }

        public void setLeftFieldItem(FieldItem leftFieldItem) {
            this.leftFieldItem = leftFieldItem;
        }

        public FieldItem getRightFieldItem() {
            return rightFieldItem;
        }

        public void setRightFieldItem(FieldItem rightFieldItem) {
            this.rightFieldItem = rightFieldItem;
        }
    }

    public static class FieldItem {

        private String fieldName;
        private String expr;
        private List<ColumnItem> columnItems = new ArrayList<>();
        private List<String> owners = new ArrayList<>();

        private boolean method;
        private boolean binaryOp;

        @Override
        public String toString() {
            return expr;
        }

        public String getColumnName() {
            return columnItems.isEmpty() ? null : columnItems.get(0).getColumnName();
        }

        public boolean containsOwnerColumn(String owner, String columnName) {
            if ("".equals(owner)) {
                owner = null;
            }
            for (ColumnItem columnItem : columnItems) {
                if (Objects.equals(owner, columnItem.getOwner())
                        && columnName.equalsIgnoreCase(columnItem.getColumnName())
                ) {
                    return true;
                }
            }
            return false;
        }

        public boolean containsOwner(Collection<String> owner) {
            for (ColumnItem columnItem : columnItems) {
                if (owner.contains(columnItem.getOwner())) {
                    return true;
                }
            }
            return false;
        }

        public boolean containsColumnName(Collection<String> columnNames) {
            for (ColumnItem columnItem : columnItems) {
                if (columnNames.contains(columnItem.getColumnName())) {
                    return true;
                }
            }
            return false;
        }

        public String getOwnerAndColumnName() {
            String owner = getOwner();
            String columnName = getColumnName();

            if (owner == null || owner.isEmpty()) {
                return columnName;
            }
            return owner + "." + columnName;
        }

        public Object getValue(Map<String, Object> data) {
            String columnName = getColumnName();
            Object o = getColumnValue(data, columnName);
            if (o == null) {
                String fieldName = getFieldName();
                o = data.get(fieldName);
            }
            return o;
        }

        public boolean equalsColumn(FieldItem fieldItem) {
            if (fieldItem.columnItems.size() != columnItems.size()) {
                return false;
            }
            for (int i = 0, size = fieldItem.columnItems.size(); i < size; i++) {
                ColumnItem columnItem = fieldItem.columnItems.get(i);
                ColumnItem thisColumnItem = columnItems.get(i);
                if (!columnItem.equalsColumnItem(thisColumnItem)) {
                    return false;
                }
            }
            return true;
        }

        public boolean equalsField(String name) {
            String columnName = getColumnName();
            String fieldName = getFieldName();

            return name.equalsIgnoreCase(columnName) || name.equalsIgnoreCase(fieldName);
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getExpr() {
            return expr;
        }

        public void setExpr(String expr) {
            this.expr = expr;
        }

        public List<ColumnItem> getColumnItems() {
            return columnItems;
        }

        public void setColumnItems(List<ColumnItem> columnItems) {
            this.columnItems = columnItems;
        }

        public boolean isMethod() {
            return method;
        }

        public void setMethod(boolean method) {
            this.method = method;
        }

        public boolean isBinaryOp() {
            return binaryOp;
        }

        public void setBinaryOp(boolean binaryOp) {
            this.binaryOp = binaryOp;
        }

        public List<String> getOwners() {
            return owners;
        }

        public void setOwners(List<String> owners) {
            this.owners = owners;
        }

        public void addColumn(String owner, ColumnItem columnItem) {
            owners.add(owner);
            columnItems.add(columnItem);
        }

        public ColumnItem getColumn() {
            if (!columnItems.isEmpty()) {
                return columnItems.get(0);
            } else {
                return null;
            }
        }

        public String getOwner() {
            if (!owners.isEmpty()) {
                return owners.get(0);
            } else {
                return null;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FieldItem fieldItem = (FieldItem) o;

            return fieldName != null ? fieldName.equals(fieldItem.fieldName) : fieldItem.fieldName == null;
        }

        @Override
        public int hashCode() {
            return fieldName != null ? fieldName.hashCode() : 0;
        }
    }

}
