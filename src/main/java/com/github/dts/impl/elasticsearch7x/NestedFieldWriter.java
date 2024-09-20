package com.github.dts.impl.elasticsearch7x;

import com.github.dts.impl.elasticsearch7x.nested.JdbcTemplateSQL;
import com.github.dts.impl.elasticsearch7x.nested.MergeJdbcTemplateSQL;
import com.github.dts.impl.elasticsearch7x.nested.SQL;
import com.github.dts.util.*;
import com.google.common.collect.Lists;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * 对象字段监听
 *
 * @author acer01
 */
public class NestedFieldWriter {
    private final Map<String, List<SchemaItem>> schemaItemMap;
    private final Map<String, List<SchemaItem>> onlyCurrentIndexSchemaItemMap;
    private final ESTemplate esTemplate;
    private final ExecutorService mainTableListenerExecutor;
    private final int threads;

    public NestedFieldWriter(int nestedFieldThreads, Map<String, ESSyncConfig> map, ESTemplate esTemplate) {
        this.schemaItemMap = toListenerMap(map, false);
        this.onlyCurrentIndexSchemaItemMap = toListenerMap(map, true);
        this.esTemplate = esTemplate;
        this.threads = nestedFieldThreads;
        this.mainTableListenerExecutor = Util.newFixedThreadPool(
                1,
                nestedFieldThreads,
                60_000L, "ESNestedMainWriter", true, false);
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
        switch (objectField.getType()) {
            case ARRAY_SQL: {
                List<Map<String, Object>> rowListCopy = ESSyncUtil.convertValueTypeCopyList(rowList, esTemplate, esMapping, objectField.getFieldName());
                //更新ES文档 (执行完会统一提交, 这里不用commit)
                esTemplate.update(esMapping, objectField.getFieldName(), pkValue, Collections.singletonMap(
                        objectField.getFieldName(), rowListCopy), bulkRequestList);
                break;
            }
            case OBJECT_SQL: {
                Map<String, Object> rowCopy = ESSyncUtil.convertValueTypeCopyMap(rowList, esTemplate, esMapping, objectField.getFieldName());
                //更新ES文档 (执行完会统一提交, 这里不用commit)
                esTemplate.update(esMapping, objectField.getFieldName(), pkValue, Collections.singletonMap(
                        objectField.getFieldName(), rowCopy), bulkRequestList);
                break;
            }
            default: {
            }
        }
    }

    /**
     * 获取所有依赖SQL
     *
     * @param dml dml数据
     * @return Dependent
     */
    private static DependentGroup getDependentList(List<SchemaItem> schemaItemList, Dml dml) {
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
        DependentGroup dependentGroup = null;
        for (SchemaItem schemaItem : schemaItemList) {
            for (int i = 0; i < size; i++) {
                Dependent dependent = new Dependent(schemaItem, i, dml);
                if (dependentGroup == null) {
                    dependentGroup = new DependentGroup();
                }
                if (dependent.isIndexMainTable()) {
                    dependentGroup.addMain(dependent);
                } else if (dependent.isNestedMainTable()) {
                    if (dependent.getSchemaItem().isJoinByMainTablePrimaryKey()) {
                        dependentGroup.addMain(dependent);
                    } else {
                        dependentGroup.addMainJoin(dependent);
                    }
                } else {
                    dependentGroup.addSlave(dependent);
                }
            }
        }
        return dependentGroup;
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
                SchemaItem schemaItem = objectField.getSchemaItem();
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


    private static Set<DependentSQL> convertToSql(List<Dependent> mainTableDependentList) {
        Set<DependentSQL> sqlList = new LinkedHashSet<>();
        for (Dependent dependent : mainTableDependentList) {
            boolean indexMainTable = dependent.isIndexMainTable();
            ESSyncConfig.ObjectField objectField = dependent.getSchemaItem().getObjectField();
            // 主表删除，nested字段会自动删除，无需处理
            if (dependent.getDml().isTypeDelete() && indexMainTable) {
                continue;
            }
            if (dependent.getDml().isTypeUpdate()) {
                // 改之前的数据也要更新
                Map<String, Object> mergeBeforeDataMap = dependent.getMergeBeforeDataMap();
                if (!mergeBeforeDataMap.isEmpty()) {
                    String fullSql = objectField.getFullSql(indexMainTable);
                    SQL sql = SQL.convertToSql(fullSql, mergeBeforeDataMap);

                    sqlList.add(new DependentSQL(sql, dependent, objectField.getSchemaItem().getGroupByIdColumns()));
                }
            }
            // 改之后的数据
            Map<String, Object> mergeAfterDataMap = dependent.getMergeAfterDataMap();
            if (!mergeAfterDataMap.isEmpty()) {
                String fullSql = objectField.getFullSql(indexMainTable);
                SQL sql = SQL.convertToSql(fullSql, mergeAfterDataMap);

                sqlList.add(new DependentSQL(sql, dependent, objectField.getSchemaItem().getGroupByIdColumns()));
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

    public DependentGroup convertToDependentGroup(List<Dml> dmls, boolean onlyCurrentIndex, boolean onlyEffect) {
        List<DependentGroup> groupList = new ArrayList<>(dmls.size());
        for (Dml dml : dmls) {
            Map<String, List<SchemaItem>> map = onlyCurrentIndex ? onlyCurrentIndexSchemaItemMap : schemaItemMap;
            DependentGroup dmlDependentGroup = getDependentList(map.get(dml.getTable()), dml);
            if (dmlDependentGroup != null) {
                groupList.add(dmlDependentGroup);
            }
        }
        return new DependentGroup(groupList, onlyEffect);
    }

    public void writeMainTable(List<Dependent> mainTableDependentList,
                               ESTemplate.BulkRequestList bulkRequestList,
                               int maxIdIn,
                               CacheMap cacheMap) {
        if (mainTableDependentList.isEmpty()) {
            return;
        }
        Set<DependentSQL> sqlList = convertToSql(mainTableDependentList);
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
        private final Dependent dependent;

        DependentSQL(SQL sql, Dependent dependent, Collection<SchemaItem.ColumnItem> needGroupBy) {
            super(sql.getExprSql(), sql.getArgs(), sql.getArgsMap(),
                    dependent.getSchemaItem().getEsMapping().getConfig().getDataSourceKey(), needGroupBy);
            this.dependent = dependent;
        }

        public Dependent getDependent() {
            return dependent;
        }

        public Object getPkValue() {
            Map<String, Object> valueMap = getArgsMap();
            String pkColumnName = getPkColumnName();
            return pkColumnName != null ? valueMap.get(pkColumnName) : null;
        }

        public String getPkColumnName() {
            ESSyncConfig.ObjectField objectField = dependent.getSchemaItem().getObjectField();
            String columnName;
            if (dependent.isIndexMainTable()) {
                columnName = objectField.getEsMapping().getSchemaItem().getIdField().getColumnName();
            } else if (dependent.getSchemaItem().isJoinByMainTablePrimaryKey()) {
                columnName = objectField.getJoinTableColumnName();
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
            return Objects.equals(dependent, that.dependent);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), dependent);
        }
    }

}
