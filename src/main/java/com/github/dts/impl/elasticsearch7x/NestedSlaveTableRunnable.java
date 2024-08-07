package com.github.dts.impl.elasticsearch7x;

import com.github.dts.impl.elasticsearch7x.NestedFieldWriter.DependentSQL;
import com.github.dts.impl.elasticsearch7x.nested.MergeJdbcTemplateSQL;
import com.github.dts.impl.elasticsearch7x.nested.SQL;
import com.github.dts.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;

class NestedSlaveTableRunnable extends CompletableFuture<Void> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(NestedSlaveTableRunnable.class);
    private final List<Dependent> updateDmlList;
    private final ES7xTemplate es7xTemplate;
    private final CacheMap cacheMap;
    private final Timestamp timestamp;
    private final JoinByParentSlaveTableForeignKey joinForeignKey = new JoinByParentSlaveTableForeignKey();
    private final JoinBySlaveTable joinBySlaveTable = new JoinBySlaveTable();

    NestedSlaveTableRunnable(List<Dependent> updateDmlList,
                             ES7xTemplate es7xTemplate,
                             int cacheMaxSize, Timestamp timestamp) {
        this.timestamp = timestamp == null ? new Timestamp(System.currentTimeMillis()) : timestamp;
        this.updateDmlList = updateDmlList;
        this.es7xTemplate = es7xTemplate;
        this.cacheMap = new CacheMap(cacheMaxSize);
    }

    @Override
    public void run() {
        int chunkSize = 1000;
        parseSql(chunkSize);
        try {
            ESTemplate.BulkRequestList bulkRequestList = es7xTemplate.newBulkRequestList(BulkPriorityEnum.LOW);
            List<MergeJdbcTemplateSQL<DependentSQL>> updateSqlList;
            if (!joinForeignKey.parentSqlList.isEmpty()) {
                updateSqlList = MergeJdbcTemplateSQL.merge(joinForeignKey.parentSqlList, chunkSize);
                Map<String, List<Map<String, Object>>> parentGetterMap = MergeJdbcTemplateSQL.toMap(updateSqlList, e->MergeJdbcTemplateSQL.argsToString(e.getArgs()),cacheMap);

                List<MergeJdbcTemplateSQL<DependentSQL>> childMergeSqlList = MergeJdbcTemplateSQL.merge(joinForeignKey.childSqlSet, chunkSize, false);
                for (MergeJdbcTemplateSQL<DependentSQL> templateSQL : childMergeSqlList) {
                    templateSQL.executeQueryStream(chunkSize, chunk -> {
                        List<Map<String, Object>> parentData = parentGetterMap.get(MergeJdbcTemplateSQL.argsToString(chunk.sql.getArgs()));
                        SchemaItem schemaItem = chunk.sql.getDependent().getSchemaItem();
                        for (Map<String, Object> row : chunk.rowList) {
                            Object pk = row.values().iterator().next();
                            NestedFieldWriter.executeEsTemplateUpdate(bulkRequestList, es7xTemplate, pk, schemaItem, parentData);
                        }
                    });
                    bulkRequestList.commit(es7xTemplate);
                }
            } else {
                updateSqlList = MergeJdbcTemplateSQL.merge(joinBySlaveTable.nestedMainSqlList, chunkSize);
                NestedFieldWriter.executeMergeUpdateES(updateSqlList, bulkRequestList, cacheMap, es7xTemplate, null, 3);
            }

            bulkRequestList.commit(es7xTemplate);
            log.info("NestedMainJoinTable={}ms, rowCount={}, dml={}, ts={}, changeSql={}",
                    System.currentTimeMillis() - timestamp.getTime(),
                    updateDmlList,
                    updateDmlList.size(),
                    timestamp, updateSqlList);
            complete(null);
        } catch (Exception e) {
            log.info("NestedMainJoinTable={}ms, rowCount={}, dml={}, ts={}, error={}",
                    System.currentTimeMillis() - timestamp.getTime(),
                    updateDmlList.size(),
                    updateDmlList,
                    timestamp, e.toString(), e);
            completeExceptionally(e);
            throw e;
        } finally {
            cacheMap.cacheClear();
        }
    }

    private void parseSql(int chunkSize) {
        for (Dependent dependent : updateDmlList) {
            Dml dml = dependent.getDml();
            if (ESSyncUtil.isEmpty(dml.getPkNames())) {
                continue;
            }
            if (dml.getOld() != null) {
                Map<String, Object> old = dml.getOld().get(dependent.getIndex());
                SchemaItem.ColumnItem columnItem = dependent.getSchemaItem().getAnyColumnItem(dml.getTable(), old.keySet());
                if (columnItem == null) {
                    continue;
                }
            }

            if (dependent.isJoinByParentSlaveTableForeignKey()) {
                Set<DependentSQL> childSqlSet = convertDependentSQLJoinByParentSlaveTable(dependent.getNestedSlaveTableList(dml.getTable()), dependent, cacheMap, chunkSize);
                if (childSqlSet.isEmpty()) {
                    continue;
                }
                Set<DependentSQL> parentSqlList = convertDependentSQL(dependent.getNestedSlaveTableList(dml.getTable()), dependent, cacheMap, chunkSize);
                if (parentSqlList.isEmpty()) {
                    continue;
                }
                joinForeignKey.childSqlSet.addAll(childSqlSet);
                joinForeignKey.parentSqlList.addAll(parentSqlList);
            } else {
                Set<DependentSQL> nestedMainSqlList = convertDependentSQL(dependent.getNestedSlaveTableList(dml.getTable()), dependent, cacheMap, chunkSize);
                if (nestedMainSqlList.isEmpty()) {
                    continue;
                }
                joinBySlaveTable.nestedMainSqlList.addAll(nestedMainSqlList);
            }
        }
    }

    private Set<DependentSQL> convertDependentSQL(List<SchemaItem.TableItem> nestedSlaveTableList, Dependent dependent, CacheMap cacheMap, int chunkSize) {
        Set<DependentSQL> byChildrenSqlSet = new LinkedHashSet<>();
        for (SchemaItem.TableItem tableItem : nestedSlaveTableList) {
            SQL nestedChildrenSql = convertNestedSlaveTableSQL(tableItem, dependent);
            byChildrenSqlSet.add(new DependentSQL(nestedChildrenSql, dependent, null));
        }

        String fullSql = dependent.getSchemaItem().getObjectField().getFullSql(false);
        Set<DependentSQL> byMainSqlList = new LinkedHashSet<>();
        MergeJdbcTemplateSQL.executeQueryList(MergeJdbcTemplateSQL.merge(byChildrenSqlSet, chunkSize), cacheMap, (sql, changeRowList) -> {
            for (Map<String, Object> changeRow : changeRowList) {
                byMainSqlList.add(new DependentSQL(SQL.convertToSql(fullSql, changeRow), dependent, dependent.getSchemaItem().getGroupByIdColumns()));
            }
        });
        return byMainSqlList;
    }

    private Set<DependentSQL> convertDependentSQLJoinByParentSlaveTable(List<SchemaItem.TableItem> nestedSlaveTableList, Dependent dependent,
                                                                        CacheMap cacheMap, int chunkSize) {
        Set<DependentSQL> byChildrenSqlSet = new LinkedHashSet<>();
        for (SchemaItem.TableItem tableItem : nestedSlaveTableList) {
            SQL nestedChildrenSql = convertNestedSlaveTableSQL(tableItem, dependent);
            byChildrenSqlSet.add(new DependentSQL(nestedChildrenSql, dependent, null));
        }

        String fullSql = getJoinByParentSlaveTableSql(dependent);
        Set<DependentSQL> byMainSqlList = new LinkedHashSet<>();
        MergeJdbcTemplateSQL.executeQueryList(MergeJdbcTemplateSQL.merge(byChildrenSqlSet, chunkSize), cacheMap, (sql, changeRowList) -> {
            for (Map<String, Object> changeRow : changeRowList) {
                byMainSqlList.add(new DependentSQL(SQL.convertToSql(fullSql, changeRow), dependent,  dependent.getSchemaItem().getGroupByIdColumns()));
            }
        });
        return byMainSqlList;
    }

    private String getJoinByParentSlaveTableSql(Dependent dependent) {
        StringBuilder condition = new StringBuilder();
        String and = " AND ";

        Map<String, List<String>> joinColumnList = getOnChildChangeWhereSqlColumnList(dependent);
        for (List<String> columnItem : joinColumnList.values()) {
            for (String column : columnItem) {
                ESSyncUtil.appendConditionByExpr(condition, "#{" + column + "}", dependent.getIndexMainTable().getAlias(), column, and);
            }
        }
        int len = condition.length();
        condition.delete(len - and.length(), len);

        SchemaItem indexMainTable = dependent.getIndexMainTable().getSchemaItem();
        SchemaItem.FieldItem idField = indexMainTable.getIdField();

        String changedSelect = SqlParser.changeSelect(indexMainTable.sql(),
                Collections.singletonMap(idField.getOwner(), Collections.singletonList(idField.getColumnName())),
                true);
        return changedSelect + " WHERE " + condition + " ";
    }

    private Map<String, List<String>> getOnChildChangeWhereSqlColumnList(Dependent dependent) {
        Map<String, List<String>> columnList = SqlParser.getColumnList(dependent.getSchemaItem().getObjectField().getOnChildChangeWhereSql());
        return columnList;
    }

    private SQL convertNestedSlaveTableSQL(SchemaItem.TableItem tableItem, Dependent dependent) {
        Dml dml = dependent.getDml();
        StringBuilder condition = new StringBuilder();
        String and = " AND ";
        for (String pkName : dml.getPkNames()) {
            ESSyncUtil.appendConditionByExpr(condition, "#{" + pkName + "}", tableItem.getAlias(), pkName, and);
        }
        int len = condition.length();
        condition.delete(len - and.length(), len);

        String sql1 = dependent.getSchemaItem().sql() + " WHERE " + condition + " ";

        Map<String, List<String>> columnList = getOnChildChangeWhereSqlColumnList(dependent);
        String sql2 = SqlParser.changeSelect(sql1, columnList, true);
        return SQL.convertToSql(sql2, dependent.getMergeDataMap());
    }

    private static class JoinByParentSlaveTableForeignKey {
        Set<DependentSQL> childSqlSet = new LinkedHashSet<>();
        Set<DependentSQL> parentSqlList = new LinkedHashSet<>();
    }

    private static class JoinBySlaveTable {
        Set<DependentSQL> nestedMainSqlList = new LinkedHashSet<>();
    }

}
