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
    private final ESTemplate esTemplate;
    private final CacheMap cacheMap;
    private final ExecutorService mainTableListenerExecutor;
    private final int threads;

    public NestedFieldWriter(int nestedFieldThreads, Map<String, ESSyncConfig> map, ESTemplate esTemplate, CacheMap cacheMap) {
        this.schemaItemMap = toListenerMap(map);
        this.esTemplate = esTemplate;
        this.cacheMap = cacheMap;
        this.threads = nestedFieldThreads;
        this.mainTableListenerExecutor = Util.newFixedThreadPool(
                1,
                nestedFieldThreads,
                60_000L, "ESNestedMainWriter", true, false);
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
                if (dependent.isMainTable()) {
                    dependentGroup.addMain(dependent);
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
    private static Map<String, List<SchemaItem>> toListenerMap(Map<String, ESSyncConfig> esSyncConfigMap) {
        Map<String, List<SchemaItem>> schemaItemMap = new LinkedHashMap<>();
        for (ESSyncConfig syncConfig : esSyncConfigMap.values()) {
            for (ESSyncConfig.ObjectField objectField : syncConfig.getEsMapping().getObjFields().values()) {
                SchemaItem schemaItem = objectField.getSchemaItem();
                if (schemaItem == null) {
                    continue;
                }
                SchemaItem.TableItem mainTable = syncConfig.getEsMapping().getSchemaItem().getMainTable();
                String mainTableName = mainTable.getTableName();
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
        return Collections.unmodifiableMap(schemaItemMap);
    }


    private static Set<DependentSQL> convertToSql(List<Dependent> mainTableDependentList, boolean autoUpdateChildren) {
        Set<DependentSQL> sqlList = new LinkedHashSet<>();
        for (Dependent dependent : mainTableDependentList) {
            ESSyncConfig.ObjectField objectField = dependent.getSchemaItem().getObjectField();
            int dmlIndex = dependent.getIndex();
            Dml dml = dependent.getDml();

            Map<String, Object> mergeDataMap = new HashMap<>();
            if (!ESSyncUtil.isEmpty(dml.getOld()) && dmlIndex < dml.getOld().size()) {
                mergeDataMap.putAll(dml.getOld().get(dmlIndex));
            }
            if (!ESSyncUtil.isEmpty(dml.getData()) && dmlIndex < dml.getData().size()) {
                mergeDataMap.putAll(dml.getData().get(dmlIndex));
            }
            if (mergeDataMap.isEmpty()) {
                continue;
            }
            String fullSql = objectField.getFullSql(dependent.isIndexMainTable());
            SQL sql = SQL.convertToSql(fullSql, mergeDataMap);
            sqlList.add(new DependentSQL(sql, dependent, autoUpdateChildren));
        }
        return sqlList;
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
            executeUpdateES(mergeSqlList, bulkRequestList, cacheMap, es7xTemplate);
        } else {
            List<List<MergeJdbcTemplateSQL<DependentSQL>>> partition = Lists.partition(new ArrayList<>(mergeSqlList),
                    Math.max(5, (mergeSqlList.size() + 1) / threads));
            if (partition.size() == 1) {
                executeUpdateES(mergeSqlList, bulkRequestList, cacheMap, es7xTemplate);
            } else {
                List<Future> futures = partition.stream()
                        .map(e -> executorService.submit(() -> executeUpdateES(e, bulkRequestList, cacheMap, es7xTemplate)))
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

    public static void executeUpdateES(List<MergeJdbcTemplateSQL<DependentSQL>> sqlList,
                                       ESTemplate.BulkRequestList bulkRequestList,
                                       CacheMap cacheMap,
                                       ESTemplate esTemplate) {
        Map<DependentSQL, List<Map<String, Object>>> batchRowGetterMap = MergeJdbcTemplateSQL.executeQueryList(sqlList, cacheMap);
        for (Map.Entry<DependentSQL, List<Map<String, Object>>> entry : batchRowGetterMap.entrySet()) {
            DependentSQL sql = entry.getKey();
            Object pkValue = sql.getPkValue();
            if (pkValue == null) {
                continue;
            }
            SchemaItem schemaItem = sql.getDependent().getSchemaItem();
            ESSyncConfig.ESMapping esMapping = schemaItem.getEsMapping();
            ESSyncConfig.ObjectField objectField = schemaItem.getObjectField();
            switch (objectField.getType()) {
                case ARRAY_SQL: {
                    List<Map<String, Object>> rowList = entry.getValue();
                    for (Map<String, Object> row : rowList) {
                        esTemplate.convertValueType(esMapping, objectField.getFieldName(), row);
                    }
                    //更新ES文档 (执行完会统一提交, 这里不用commit)
                    esTemplate.update(esMapping, pkValue, Collections.singletonMap(
                            objectField.getFieldName(), rowList), bulkRequestList);
                    break;
                }
                case OBJECT_SQL: {
                    List<Map<String, Object>> rowList = entry.getValue();
                    Map<String, Object> resultMap = rowList == null || rowList.isEmpty() ? null : rowList.get(0);
                    if (resultMap != null) {
                        esTemplate.convertValueType(esMapping, objectField.getFieldName(), resultMap);
                    }
                    //更新ES文档 (执行完会统一提交, 这里不用commit)
                    esTemplate.update(esMapping, pkValue, Collections.singletonMap(
                            objectField.getFieldName(), resultMap), bulkRequestList);
                    break;
                }
                default: {
                }
            }
        }
    }

    public DependentGroup convertToDependentGroup(List<Dml> dmls) {
        DependentGroup dependentGroup = new DependentGroup();
        for (Dml dml : dmls) {
            if (Boolean.TRUE.equals(dml.getIsDdl())) {
                continue;
            }
            DependentGroup dmlDependentGroup = getDependentList(schemaItemMap.get(dml.getTable()), dml);
            if (dmlDependentGroup != null) {
                dependentGroup.add(dmlDependentGroup);
            }
        }
        return dependentGroup;
    }

    public void writeMainTable(List<Dependent> mainTableDependentList,
                               ESTemplate.BulkRequestList bulkRequestList,
                               boolean autoUpdateChildren) {
        Set<DependentSQL> sqlList = convertToSql(mainTableDependentList, autoUpdateChildren);
        List<MergeJdbcTemplateSQL<DependentSQL>> mergeSqlList = MergeJdbcTemplateSQL.merge(sqlList, 1000, false);
        executeMergeUpdateES(mergeSqlList, bulkRequestList, cacheMap, esTemplate, mainTableListenerExecutor, threads);
    }

    public static class DependentSQL extends JdbcTemplateSQL {
        private final Dependent dependent;
        private final boolean autoUpdateChildren;

        DependentSQL(SQL sql, Dependent dependent, boolean autoUpdateChildren) {
            super(sql.getExprSql(), sql.getArgs(), sql.getArgsMap(),
                    dependent.getSchemaItem().getEsMapping().getConfig().getDataSourceKey());
            this.dependent = dependent;
            this.autoUpdateChildren = autoUpdateChildren;
        }

        public Dependent getDependent() {
            return dependent;
        }

        public Object getPkValue() {
            ESSyncConfig.ObjectField objectField = dependent.getSchemaItem().getObjectField();
            ESSyncConfig.ESMapping esMapping = objectField.getEsMapping();
            Map<String, Object> valueMap = getArgsMap();
            Object pkValue;
            if (dependent.isIndexMainTable()) {
                if (autoUpdateChildren) {
                    pkValue = valueMap.containsKey(esMapping.getPk()) ?
                            valueMap.get(esMapping.getPk()) : valueMap.get(esMapping.get_id());
                } else {
                    pkValue = null;
                }
            } else {
                pkValue = valueMap.get(objectField.getParentDocumentId());
            }
            return pkValue;
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
