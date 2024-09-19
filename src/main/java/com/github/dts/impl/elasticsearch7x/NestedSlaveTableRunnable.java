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
    private final Timestamp timestamp;
    private final JoinByParentSlaveTableForeignKey joinForeignKey = new JoinByParentSlaveTableForeignKey();
    private final JoinBySlaveTable joinBySlaveTable = new JoinBySlaveTable();
    private final ESTemplate.BulkRequestList bulkRequestList;
    private final ESTemplate.CommitListener commitListener;
    private final CacheMap cacheMap;
    private final int chunkSize;
    private final int streamChunkSize;
    private final NestedSlaveTableRunnable oldRun;
    private final NestedSlaveTableRunnable newRun;

    NestedSlaveTableRunnable(List<Dependent> updateDmlList,
                             ES7xTemplate es7xTemplate,
                             ESTemplate.BulkRequestList bulkRequestList,
                             ESTemplate.CommitListener commitListener,
                             CacheMap cacheMap, Timestamp timestamp, int chunkSize, int streamChunkSize) {
        this(updateDmlList, es7xTemplate, bulkRequestList, commitListener, cacheMap, timestamp, chunkSize, streamChunkSize, null, null);
    }

    NestedSlaveTableRunnable(List<Dependent> updateDmlList,
                             ES7xTemplate es7xTemplate,
                             ESTemplate.BulkRequestList bulkRequestList,
                             ESTemplate.CommitListener commitListener,
                             CacheMap cacheMap, Timestamp timestamp, int chunkSize, int streamChunkSize,
                             NestedSlaveTableRunnable oldRun, NestedSlaveTableRunnable newRun) {
        this.timestamp = timestamp == null ? new Timestamp(System.currentTimeMillis()) : timestamp;
        this.updateDmlList = updateDmlList;
        this.es7xTemplate = es7xTemplate;
        this.chunkSize = chunkSize;
        this.bulkRequestList = bulkRequestList;
        this.commitListener = commitListener;
        this.streamChunkSize = streamChunkSize;
        this.cacheMap = cacheMap;
        this.oldRun = oldRun;
        this.newRun = newRun;
    }

    static NestedSlaveTableRunnable merge(NestedSlaveTableRunnable oldRun, NestedSlaveTableRunnable newRun) {
        List<Dependent> dmlList = new ArrayList<>(oldRun.updateDmlList.size() + newRun.updateDmlList.size());
        dmlList.addAll(oldRun.updateDmlList);
        dmlList.addAll(newRun.updateDmlList);
        return new NestedSlaveTableRunnable(dmlList, oldRun.es7xTemplate,
                oldRun.bulkRequestList.fork(newRun.bulkRequestList),
                ESTemplate.merge(oldRun.commitListener, newRun.commitListener),
                oldRun.cacheMap, oldRun.timestamp, oldRun.chunkSize, oldRun.streamChunkSize,
                oldRun, newRun);
    }

    static String id(Dependent id, String key) {
        return id.dmlKey() + "_" + key;
    }

    @Override
    public void run() {
        parseSql(chunkSize, cacheMap);
        try {
            ESTemplate.BulkRequestList bulkRequestList = this.bulkRequestList.fork(BulkPriorityEnum.LOW);
            if (!joinBySlaveTable.nestedMainSqlList.isEmpty()) {
                List<MergeJdbcTemplateSQL<DependentSQL>> merge = MergeJdbcTemplateSQL.merge(joinBySlaveTable.nestedMainSqlList, chunkSize);
                MergeJdbcTemplateSQL.executeQueryList(merge, cacheMap, (sql, list) -> NestedFieldWriter.executeEsTemplateUpdate(bulkRequestList, es7xTemplate, sql, list));
            }

            if (!joinForeignKey.parentSqlList.isEmpty()) {
                List<MergeJdbcTemplateSQL<DependentSQL>> merge = MergeJdbcTemplateSQL.merge(joinForeignKey.parentSqlList, chunkSize);
                Map<String, List<Map<String, Object>>> parentGetterMap = MergeJdbcTemplateSQL.toMap(merge, e -> id(e.getDependent(), MergeJdbcTemplateSQL.argsToString(e.getArgs())), cacheMap);
                List<MergeJdbcTemplateSQL<DependentSQL>> childMergeSqlList = MergeJdbcTemplateSQL.merge(joinForeignKey.childSqlSet, chunkSize, false);
                Set<String> visited = new HashSet<>(parentGetterMap.size());
                for (MergeJdbcTemplateSQL<DependentSQL> templateSQL : childMergeSqlList) {
                    templateSQL.executeQueryStream(streamChunkSize, DependentSQL::getDependent, chunk -> {
                        String dmlUniqueColumnKey = id(chunk.source, chunk.uniqueColumnKey);
                        List<Object> pkList = chunk.rowListFirst(visited, dmlUniqueColumnKey);
                        if (!pkList.isEmpty()) {
                            List<Map<String, Object>> parentData = parentGetterMap.get(dmlUniqueColumnKey);
                            SchemaItem schemaItem = chunk.source.getSchemaItem();
                            NestedFieldWriter.executeEsTemplateUpdate(bulkRequestList, es7xTemplate, pkList, schemaItem, parentData);
                        }
                    });
                    bulkRequestList.commit(es7xTemplate);
                }
            }

            bulkRequestList.commit(es7xTemplate);
            log.info("NestedMainJoinTable={}ms, rowCount={}, dml={}, ts={}",
                    System.currentTimeMillis() - timestamp.getTime(),
                    updateDmlList,
                    updateDmlList.size(),
                    timestamp);
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

    private void parseSql(int chunkSize, CacheMap cacheMap) {
        for (Dependent dependent : updateDmlList) {
            Dml dml = dependent.getDml();
            if (ESSyncUtil.isEmpty(dml.getPkNames())) {
                continue;
            }

            if (dependent.isJoinByMainTablePrimaryKey()) {
                Set<DependentSQL> nestedMainSqlList = convertDependentSQL(dependent.getNestedSlaveTableList(dml.getTable()), dependent, cacheMap, chunkSize);
                if (nestedMainSqlList.isEmpty()) {
                    continue;
                }
                joinBySlaveTable.nestedMainSqlList.addAll(nestedMainSqlList);
            } else {
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
                byMainSqlList.add(new DependentSQL(SQL.convertToSql(fullSql, changeRow), dependent, dependent.getSchemaItem().getGroupByIdColumns()));
            }
        });
        return byMainSqlList;
    }

    private String getJoinByParentSlaveTableSql(Dependent dependent) {
        StringBuilder condition = new StringBuilder();
        String and = " AND ";

        Map<String, List<String>> joinColumnList = dependent.getSchemaItem().getOnSlaveTableChangeWhereSqlColumnList();
        for (List<String> columnItem : joinColumnList.values()) {
            for (String column : columnItem) {
                ESSyncUtil.appendConditionByExpr(condition, SQL.wrapPlaceholder(column), dependent.getIndexMainTable().getAlias(), column, and);
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

    private SQL convertNestedSlaveTableSQL(SchemaItem.TableItem tableItem, Dependent dependent) {
        Dml dml = dependent.getDml();
        StringBuilder condition = new StringBuilder();
        String and = " AND ";
        for (String pkName : dml.getPkNames()) {
            ESSyncUtil.appendConditionByExpr(condition, SQL.wrapPlaceholder(pkName), tableItem.getAlias(), pkName, and);
        }
        int len = condition.length();
        condition.delete(len - and.length(), len);

        String sql1 = dependent.getSchemaItem().sql() + " WHERE " + condition + " ";

        Map<String, List<String>> columnList = dependent.getSchemaItem().getOnSlaveTableChangeWhereSqlColumnList();
        String sql2 = SqlParser.changeSelect(sql1, columnList, true);
        return SQL.convertToSql(sql2, dependent.getMergeDataMap());
    }

    @Override
    public boolean complete(Void value) {
        if (oldRun != null) {
            oldRun.complete(value);
        }
        if (newRun != null) {
            newRun.complete(value);
        }
        return super.complete(value);
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        if (oldRun != null) {
            oldRun.completeExceptionally(ex);
        }
        if (newRun != null) {
            newRun.completeExceptionally(ex);
        }
        return super.completeExceptionally(ex);
    }

    private static class JoinByParentSlaveTableForeignKey {
        Set<DependentSQL> childSqlSet = new LinkedHashSet<>();
        Set<DependentSQL> parentSqlList = new LinkedHashSet<>();
    }

    private static class JoinBySlaveTable {
        Set<DependentSQL> nestedMainSqlList = new LinkedHashSet<>();
    }
}
