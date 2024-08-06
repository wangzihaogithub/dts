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
    private final CacheMap cacheMap;
    private final ExecutorService mainTableListenerExecutor;
    private final int threads;

    public NestedFieldWriter(int nestedFieldThreads, Map<String, ESSyncConfig> map, ESTemplate esTemplate, CacheMap cacheMap) {
        this.schemaItemMap = toListenerMap(map, false);
        this.onlyCurrentIndexSchemaItemMap = toListenerMap(map, true);
        this.esTemplate = esTemplate;
        this.cacheMap = cacheMap;
        this.threads = nestedFieldThreads;
        this.mainTableListenerExecutor = Util.newFixedThreadPool(
                1,
                nestedFieldThreads,
                60_000L, "ESNestedMainWriter", true, false);
    }

    public static void executeMergeUpdateES(List<MergeJdbcTemplateSQL<DependentSQL>> mergeSqlList,
                                            ESTemplate.BulkRequestList bulkRequestList,
                                            CacheMap cacheMap,
                                            ESTemplate es7xTemplate,
                                            ExecutorService executorService,
                                            int threads) {
        if (mergeSqlList.isEmpty()) {
            return;
        }
        if (executorService == null) {
            executeEsTemplateUpdate(mergeSqlList, bulkRequestList, cacheMap, es7xTemplate);
        } else {
            List<List<MergeJdbcTemplateSQL<DependentSQL>>> partition = Lists.partition(new ArrayList<>(mergeSqlList),
                    Math.max(5, (mergeSqlList.size() + 1) / threads));
            if (partition.size() == 1) {
                executeEsTemplateUpdate(mergeSqlList, bulkRequestList, cacheMap, es7xTemplate);
            } else {
                List<Future> futures = partition.stream()
                        .map(e -> executorService.submit(() -> executeEsTemplateUpdate(e, bulkRequestList, cacheMap, es7xTemplate)))
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
                List<Map<String, Object>> rowListCopy = new ArrayList<>();
                if (rowList != null) {
                    for (Map<String, Object> row : rowList) {
                        Map<String, Object> rowCopy = new LinkedHashMap<>(row);
                        esTemplate.convertValueType(esMapping, objectField.getFieldName(), rowCopy);
                        rowListCopy.add(rowCopy);
                    }
                }
                //更新ES文档 (执行完会统一提交, 这里不用commit)
                esTemplate.update(esMapping, objectField.getFieldName(), pkValue, Collections.singletonMap(
                        objectField.getFieldName(), rowListCopy), bulkRequestList);
                break;
            }
            case OBJECT_SQL: {
                Map<String, Object> rowCopy;
                if (rowList != null && !rowList.isEmpty()) {
                    rowCopy = new LinkedHashMap<>(rowList.get(0));
                    esTemplate.convertValueType(esMapping, objectField.getFieldName(), rowCopy);
                } else {
                    rowCopy = null;
                }
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
        DependentGroup dependentGroup = new DependentGroup();
        for (SchemaItem schemaItem : schemaItemList) {
            SchemaItem.TableItem indexMainTable = schemaItem.getObjectField().getEsMapping().getSchemaItem().getMainTable();
            SchemaItem.TableItem nestedMainTable = schemaItem.getObjectField().getSchemaItem().getMainTable();
            List<SchemaItem.TableItem> nestedSlaveTableList = schemaItem.getObjectField().getSchemaItem().getSlaveTableList();

            for (int i = 0; i < size; i++) {
                Dependent dependent = new Dependent(schemaItem, i,
                        indexMainTable, nestedMainTable, nestedSlaveTableList,
                        dml);
                if (dependent.isIndexMainTable()) {
                    dependentGroup.addMain(dependent);
                } else if (dependent.isNestedMainTable()) {
                    if (dependent.getSchemaItem().isJoinByParentPrimaryKey()) {
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
            ESSyncConfig.ObjectField objectField = dependent.getSchemaItem().getObjectField();
            Map<String, Object> mergeDataMap = dependent.getMergeDataMap();
            if (mergeDataMap.isEmpty()) {
                continue;
            }
            String fullSql = objectField.getFullSql(dependent.isIndexMainTable());
            SQL sql = SQL.convertToSql(fullSql, mergeDataMap);

            sqlList.add(new DependentSQL(sql, dependent, objectField.getSchemaItem().getGroupByIdColumns()));
        }
        return sqlList;
    }

    private static void executeEsTemplateUpdate(List<MergeJdbcTemplateSQL<DependentSQL>> sqlList,
                                                ESTemplate.BulkRequestList bulkRequestList,
                                                CacheMap cacheMap,
                                                ESTemplate esTemplate) {
        MergeJdbcTemplateSQL.executeQueryList(sqlList, cacheMap, (sql, list) -> executeEsTemplateUpdate(bulkRequestList, esTemplate, sql, list));
    }

    public DependentGroup convertToDependentGroup(List<Dml> dmls, boolean onlyCurrentIndex) {
        DependentGroup dependentGroup = new DependentGroup();
        for (Dml dml : dmls) {
            if (Boolean.TRUE.equals(dml.getIsDdl())) {
                continue;
            }
            Map<String, List<SchemaItem>> map = onlyCurrentIndex ? onlyCurrentIndexSchemaItemMap : schemaItemMap;
            DependentGroup dmlDependentGroup = getDependentList(map.get(dml.getTable()), dml);
            if (dmlDependentGroup != null) {
                dependentGroup.add(dmlDependentGroup);
            }
        }
        return dependentGroup;
    }

    public void writeMainTable(List<Dependent> mainTableDependentList,
                               ESTemplate.BulkRequestList bulkRequestList,
                               boolean autoUpdateChildren) {
        Set<DependentSQL> sqlList = convertToSql(mainTableDependentList);
        List<MergeJdbcTemplateSQL<DependentSQL>> mergeSqlList = MergeJdbcTemplateSQL.merge(sqlList, 1000);
        executeMergeUpdateES(mergeSqlList, bulkRequestList, cacheMap, esTemplate, mainTableListenerExecutor, threads);
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
            } else if (dependent.getSchemaItem().isJoinByParentPrimaryKey()) {
                columnName = objectField.getParentDocumentId();
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
