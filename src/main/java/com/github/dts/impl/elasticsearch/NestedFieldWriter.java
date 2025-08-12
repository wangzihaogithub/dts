package com.github.dts.impl.elasticsearch;

import com.github.dts.impl.elasticsearch.nested.JdbcTemplateSQL;
import com.github.dts.impl.elasticsearch.nested.MergeJdbcTemplateSQL;
import com.github.dts.impl.elasticsearch.nested.SQL;
import com.github.dts.util.*;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * 对象字段监听
 *
 * @author acer01
 * @see NestedFieldWriter#convertToSqlDependentGroup(List, boolean, boolean)
 */
public class NestedFieldWriter {
    private final Map<String, List<SchemaItem>> schemaItemMap;
    private final Map<String, List<SchemaItem>> onlyCurrentIndexSchemaItemMap;
    private final ESTemplate esTemplate;
    private final ExecutorService mainTableListenerExecutor;
    private final int threads;

    public NestedFieldWriter(int nestedFieldThreads, String name, Map<String, ESSyncConfig> map, ESTemplate esTemplate) {
        this.schemaItemMap = toListenerMap(map, false);
        this.onlyCurrentIndexSchemaItemMap = toListenerMap(map, true);
        this.esTemplate = esTemplate;
        this.threads = nestedFieldThreads;
        this.mainTableListenerExecutor = Util.newFixedThreadPool(
                1,
                nestedFieldThreads,
                60_000L, name, true, false);
    }

    public static void executeEsTemplateUpdate(ESTemplate.BulkRequestList bulkRequestList,
                                               ESTemplate esTemplate,
                                               DependentSQL sql,
                                               List<Map<String, Object>> rowList) {
        executeEsTemplateUpdate(bulkRequestList, esTemplate, sql.getPkValue(), sql.getDependent().getSchemaItem(), rowList);
    }

    public static void executeEsTemplateUpdate(ESTemplate.BulkRequestList bulkRequestList,
                                               ESTemplate esTemplate,
                                               Object pkValue,
                                               SchemaItem schemaItem,
                                               List<Map<String, Object>> rowList) {
        if (pkValue == null) {
            return;
        }
        ESSyncConfig.ESMapping esMapping = schemaItem.getEsMapping();
        ESSyncConfig.ObjectField objectField = schemaItem.getObjectField();
        if (!objectField.isSqlType()) {
            return;
        }
        Object mysqlValue = EsGetterUtil.getSqlObjectMysqlValue(objectField, rowList, esTemplate, esMapping);
        Map<String, Object> mysqlData = Collections.singletonMap(objectField.getFieldName(), mysqlValue);
        esTemplate.update(esMapping, pkValue, mysqlData, bulkRequestList);//Nested DML
    }

    /**
     * 获取所有依赖SQL
     *
     * @param dml dml数据
     * @return Dependent
     */
    private static SqlDependentGroup getDependentList(List<SchemaItem> schemaItemList, Dml dml) {
        if (schemaItemList == null || schemaItemList.isEmpty()) {
            return null;
        }
        //当前表所依赖的所有sql
        List<Map<String, Object>> oldList = dml.getOld();
        if (oldList == null) {
            oldList = Collections.emptyList();
        }
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null) {
            dataList = Collections.emptyList();
        }

        int size = Math.max(oldList.size(), dataList.size());
        SqlDependentGroup sqlDependentGroup = null;
        for (SchemaItem schemaItem : schemaItemList) {
            for (int i = 0; i < size; i++) {
                SqlDependent sqlDependent = new SqlDependent(schemaItem, i, dml);
                if (sqlDependentGroup == null) {
                    sqlDependentGroup = new SqlDependentGroup();
                }
                if (sqlDependent.isIndexMainTable()) {
                    sqlDependentGroup.addMain(sqlDependent);
                } else if (sqlDependent.isNestedMainTable()) {
                    if (sqlDependent.getSchemaItem().isJoinByMainTablePrimaryKey()) {
                        sqlDependentGroup.addMain(sqlDependent);
                    } else {
                        sqlDependentGroup.addMainJoin(sqlDependent);
                    }
                } else {
                    sqlDependentGroup.addSlave(sqlDependent);
                }
            }
        }
        return sqlDependentGroup;
    }

    /**
     * 初始化监听表与sql的关系
     *
     * @return 表与sql的关系
     */
    private static Map<String, List<SchemaItem>> toListenerMap(Map<String, ESSyncConfig> esSyncConfigMap, boolean onlyCurrentIndex) {
        Map<String, List<SchemaItem>> schemaItemMap = new LinkedHashMap<>();
        for (ESSyncConfig syncConfig : esSyncConfigMap.values()) {
            for (ESSyncConfig.ObjectField objectField : syncConfig.getEsMapping().getObjFields().values()) {
                ESSyncConfig.ObjectField.ParamSql paramSql = objectField.getParamSql();
                if (paramSql == null) {
                    continue;
                }
                SchemaItem schemaItem = paramSql.getSchemaItem();
                if (schemaItem == null) {
                    continue;
                }
                SchemaItem.TableItem mainTable = syncConfig.getEsMapping().getSchemaItem().getMainTable();
                String mainTableName = mainTable.getTableName();
                if (onlyCurrentIndex) {
                    schemaItemMap.computeIfAbsent(mainTableName, e -> new ArrayList<>(2))
                            .add(schemaItem);
                } else {
                    Set<String> tableNameSet = schemaItem.getTableItemAliases().keySet();
                    for (String tableName : tableNameSet) {
                        schemaItemMap.computeIfAbsent(tableName, e -> new ArrayList<>(2))
                                .add(schemaItem);
                    }
                    if (!tableNameSet.contains(mainTableName)) {
                        schemaItemMap.computeIfAbsent(mainTableName, e -> new ArrayList<>(2))
                                .add(schemaItem);
                    }
                }
            }
        }
        return Collections.unmodifiableMap(schemaItemMap);
    }


    private static Set<DependentSQL> convertToSql(List<SqlDependent> mainTableSqlDependentList) {
        Set<DependentSQL> sqlList = new LinkedHashSet<>();
        for (SqlDependent sqlDependent : mainTableSqlDependentList) {
            boolean indexMainTable = sqlDependent.isIndexMainTable();
            ESSyncConfig.ObjectField objectField = sqlDependent.getSchemaItem().getObjectField();
            // 主表删除，nested字段会自动删除，无需处理
            if (sqlDependent.getDml().isTypeDelete() && indexMainTable) {
                continue;
            }
            if (sqlDependent.getDml().isTypeUpdate()) {
                // 改之前的数据也要更新
                Map<String, Object> mergeBeforeDataMap = sqlDependent.getMergeBeforeDataMap();
                if (!mergeBeforeDataMap.isEmpty()) {
                    ESSyncConfig.ObjectField.ParamSql paramSql = objectField.getParamSql();
                    if (paramSql != null) {
                        String fullSql = paramSql.getFullSql(indexMainTable);
                        SQL sql = SQL.convertToSql(fullSql, mergeBeforeDataMap);
                        sqlList.add(new DependentSQL(sql, sqlDependent, paramSql.getSchemaItem().getGroupByIdColumns()));
                    }
                }
            }
            // 改之后的数据
            Map<String, Object> mergeAfterDataMap = sqlDependent.getMergeAfterDataMap();
            if (!mergeAfterDataMap.isEmpty()) {
                ESSyncConfig.ObjectField.ParamSql paramSql = objectField.getParamSql();
                if (paramSql != null) {
                    String fullSql = paramSql.getFullSql(indexMainTable);
                    SQL sql = SQL.convertToSql(fullSql, mergeAfterDataMap);
                    sqlList.add(new DependentSQL(sql, sqlDependent, paramSql.getSchemaItem().getGroupByIdColumns()));
                }
            }
        }
        return sqlList;
    }

    private static void executeEsTemplateUpdate(List<MergeJdbcTemplateSQL<DependentSQL>> sqlList,
                                                ESTemplate.BulkRequestList bulkRequestList,
                                                CacheMap cacheMap,
                                                ESTemplate esTemplate) {
        MergeJdbcTemplateSQL.executeQueryList(sqlList, cacheMap, (sql, list) -> executeEsTemplateUpdate(bulkRequestList, esTemplate, sql, list));
    }

    public SqlDependentGroup convertToSqlDependentGroup(List<Dml> dmls, boolean onlyCurrentIndex, boolean onlyEffect) {
        List<SqlDependentGroup> groupList = new ArrayList<>(dmls.size());
        for (Dml dml : dmls) {
            Map<String, List<SchemaItem>> map = onlyCurrentIndex ? onlyCurrentIndexSchemaItemMap : schemaItemMap;
            SqlDependentGroup dmlSqlDependentGroup = getDependentList(map.get(dml.getTable()), dml);
            if (dmlSqlDependentGroup != null) {
                groupList.add(dmlSqlDependentGroup);
            }
        }
        return new SqlDependentGroup(groupList, onlyEffect);
    }

    public void writeMainTable(List<SqlDependent> mainTableSqlDependentList,
                               ESTemplate.BulkRequestList bulkRequestList,
                               int maxIdIn,
                               CacheMap cacheMap) {
        if (mainTableSqlDependentList.isEmpty()) {
            return;
        }
        Set<DependentSQL> sqlList = convertToSql(mainTableSqlDependentList);
        List<MergeJdbcTemplateSQL<DependentSQL>> mergeSqlList = MergeJdbcTemplateSQL.merge(sqlList, maxIdIn);
        if (mainTableListenerExecutor == null) {
            executeEsTemplateUpdate(mergeSqlList, bulkRequestList, cacheMap, esTemplate);
        } else {
            List<List<MergeJdbcTemplateSQL<DependentSQL>>> partition = Lists.partition(new ArrayList<>(mergeSqlList),
                    Math.max(5, (mergeSqlList.size() + 1) / threads));
            if (partition.size() == 1) {
                executeEsTemplateUpdate(mergeSqlList, bulkRequestList, cacheMap, esTemplate);
            } else {
                List<Future> futures = partition.stream()
                        .map(e -> mainTableListenerExecutor.submit(() -> executeEsTemplateUpdate(e, bulkRequestList, cacheMap, esTemplate)))
                        .collect(Collectors.toList());
                for (Future future : futures) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        Util.sneakyThrows(e);
                    }
                }
            }
        }
    }

    public static class DependentSQL extends JdbcTemplateSQL {
        private final SqlDependent sqlDependent;

        DependentSQL(SQL sql, SqlDependent sqlDependent, Collection<ColumnItem> needGroupBy) {
            super(sql.getExprSql(), sql.getArgs(), sql.getArgsMap(),
                    sqlDependent.getSchemaItem().getEsMapping().getConfig().getDataSourceKey(), needGroupBy);
            this.sqlDependent = sqlDependent;
        }

        public SqlDependent getDependent() {
            return sqlDependent;
        }

        public Object getPkValue() {
            Map<String, Object> valueMap = getArgsMap();
            String pkColumnName = getPkColumnName();
            return pkColumnName != null ? valueMap.get(pkColumnName) : null;
        }

        public String getPkColumnName() {
            ESSyncConfig.ObjectField objectField = sqlDependent.getSchemaItem().getObjectField();
            String columnName;
            if (sqlDependent.isIndexMainTable()) {
                columnName = objectField.getEsMapping().getSchemaItem().getIdField().getColumnName();
            } else if (sqlDependent.getSchemaItem().isJoinByMainTablePrimaryKey()) {
                columnName = objectField.getParamSql().getJoinTableColumnName();
            } else {
                columnName = null;
            }
            return columnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DependentSQL)) return false;
            if (!super.equals(o)) return false;
            DependentSQL that = (DependentSQL) o;
            return Objects.equals(sqlDependent, that.sqlDependent);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), sqlDependent);
        }
    }

}
