package com.github.dts.impl.elasticsearch;

import com.github.dts.impl.elasticsearch.NestedFieldWriter.DependentSQL;
import com.github.dts.impl.elasticsearch.nested.MergeJdbcTemplateSQL;
import com.github.dts.impl.elasticsearch.nested.SQL;
import com.github.dts.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Nested字段的从表发生变化时，对应的处理逻辑
 * 例如下配置：
 * corp_name就是从表，它发生变化后会触发逻辑。
 * <pre>
 *     esMapping:
 *      env: test
 *      _index: cnwy_job_test_index_alias
 *      sql: "SELECT
 *              job.id as id,
 *              job.`name` as name
 *            FROM job job
 *       "
 *     corp:
 *       type: object-sql
 *       paramSql:
 *         sql: "SELECT
 *                 corp.id ,
 *                 corp.`name`,
 *                 GROUP_CONCAT(if(corpName.type = 2, corpName.`name`, null)) as aliasNames,
 *                 GROUP_CONCAT(if(corpName.type = 3, corpName.`name`, null)) as historyNames
 *               FROM corp corp
 *               LEFT JOIN corp_name corpName on corpName.corp_id = corp.id
 *         "
 *         onMainTableChangeWhereSql: 'WHERE corp.id = #{corp_id} '
 *         onSlaveTableChangeWhereSql: 'WHERE corp.id = #{id} '
 *
 * </pre>
 *
 * @see NestedFieldWriter#convertToSqlDependentGroup(List, boolean, boolean)
 */
class NestedSlaveTableRunnable extends CompletableFuture<Void> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(NestedSlaveTableRunnable.class);
    private final List<SqlDependent> updateDmlList;
    private final DefaultESTemplate esTemplate;
    private final Timestamp timestamp;
    private final JoinByParentSlaveTableForeignKey joinForeignKey = new JoinByParentSlaveTableForeignKey();
    private final JoinBySlaveTable joinBySlaveTable = new JoinBySlaveTable();
    private final ESTemplate.BulkRequestList parentBulkRequestList;
    private final ESTemplate.BulkRequestList bulkRequestList;
    private final ESTemplate.CommitListener commitListener;
    private final CacheMap cacheMap;
    private final int chunkSize;
    private final int streamChunkSize;
    private final NestedSlaveTableRunnable oldRun;
    private final NestedSlaveTableRunnable newRun;
    private List<MergeJdbcTemplateSQL<DependentSQL>> nestedMainSqlListMerge;
    private List<MergeJdbcTemplateSQL<DependentSQL>> joinForeignKeyMerge;

    NestedSlaveTableRunnable(List<SqlDependent> updateDmlList,
                             DefaultESTemplate esTemplate,
                             ESTemplate.BulkRequestList bulkRequestList,
                             ESTemplate.CommitListener commitListener,
                             CacheMap cacheMap, Timestamp timestamp, int chunkSize, int streamChunkSize) {
        this(updateDmlList, esTemplate, bulkRequestList, commitListener, cacheMap, timestamp, chunkSize, streamChunkSize, null, null);
    }

    NestedSlaveTableRunnable(List<SqlDependent> updateDmlList,
                             DefaultESTemplate esTemplate,
                             ESTemplate.BulkRequestList bulkRequestList,
                             ESTemplate.CommitListener commitListener,
                             CacheMap cacheMap, Timestamp timestamp, int chunkSize, int streamChunkSize,
                             NestedSlaveTableRunnable oldRun, NestedSlaveTableRunnable newRun) {
        this.timestamp = timestamp == null ? new Timestamp(System.currentTimeMillis()) : timestamp;
        this.updateDmlList = updateDmlList;
        this.esTemplate = esTemplate;
        this.chunkSize = chunkSize;
        this.parentBulkRequestList = bulkRequestList;
        this.bulkRequestList = bulkRequestList.fork(BulkPriorityEnum.LOW);
        this.commitListener = commitListener;
        this.streamChunkSize = streamChunkSize;
        this.cacheMap = cacheMap;
        this.oldRun = oldRun;
        this.newRun = newRun;
    }

    static NestedSlaveTableRunnable merge(NestedSlaveTableRunnable oldRun, NestedSlaveTableRunnable newRun) {
        List<SqlDependent> dmlList = new ArrayList<>(oldRun.updateDmlList.size() + newRun.updateDmlList.size());
        dmlList.addAll(oldRun.updateDmlList);
        dmlList.addAll(newRun.updateDmlList);
        return new NestedSlaveTableRunnable(dmlList, oldRun.esTemplate,
                oldRun.parentBulkRequestList.fork(newRun.parentBulkRequestList),
                ESTemplate.merge(oldRun.commitListener, newRun.commitListener),
                oldRun.cacheMap, oldRun.timestamp, oldRun.chunkSize, oldRun.streamChunkSize,
                oldRun, newRun);
    }

    static String id(SqlDependent id, String key) {
        return id.dmlKey() + "_" + key;
    }

    static class Executor {
        private final AtomicInteger idIncr = new AtomicInteger();
        private final java.util.concurrent.Executor executor;
        private final Map<Integer, NestedSlaveTableRunnable> runnableMap = new ConcurrentHashMap<>();

        Executor(java.util.concurrent.Executor executor) {
            this.executor = executor;
        }

        void execute(NestedSlaveTableRunnable runnable) {
            trace(runnable);
            executor.execute(runnable);
        }

        void trace(NestedSlaveTableRunnable runnable) {
            int id = idIncr.incrementAndGet();
            runnableMap.put(id, runnable);
            runnable.whenComplete((unused, throwable) -> runnableMap.remove(id));
        }

        EsSyncRunnableStatus status() {
            EsSyncRunnableStatus status = new EsSyncRunnableStatus();
            for (NestedSlaveTableRunnable value : runnableMap.values()) {
                List<MergeJdbcTemplateSQL<DependentSQL>> nestedMainSqlListMerge = value.nestedMainSqlListMerge;
                List<MergeJdbcTemplateSQL<DependentSQL>> joinForeignKeyMerge = value.joinForeignKeyMerge;

                EsSyncRunnableStatus.Row row = new EsSyncRunnableStatus.Row();
                row.setTotal(value.joinBySlaveTable.nestedMainSqlList.size() + value.joinForeignKey.childSqlSet.size());
                row.setCurr(value.bulkRequestList.commitRequests());
                row.setBinlogTimestamp(String.valueOf(value.timestamp));
                Map<String, Object> info = new LinkedHashMap<>(3);
                info.put("updateDmlList", value.updateDmlList.stream().map(SqlDependent::toString).collect(Collectors.toList()));
                info.put("nestedMainSqlListMerge", nestedMainSqlListMerge == null ? null : nestedMainSqlListMerge.stream().map(SQL::toString).collect(Collectors.toList()));
                info.put("joinForeignKeyMerge", joinForeignKeyMerge == null ? null : joinForeignKeyMerge.stream().map(SQL::toString).collect(Collectors.toList()));
                row.setInfo(info);
                status.getRows().add(row);
            }
            return status;
        }
    }

    @Override
    public void run() {
        parseSql(chunkSize, cacheMap);
        try {
            List<MergeJdbcTemplateSQL<DependentSQL>> nestedMainSqlListMerge = this.nestedMainSqlListMerge = joinBySlaveTable.nestedMainSqlList.isEmpty() ?
                    Collections.emptyList() : MergeJdbcTemplateSQL.merge(joinBySlaveTable.nestedMainSqlList, chunkSize);
            if (!nestedMainSqlListMerge.isEmpty()) {
                MergeJdbcTemplateSQL.executeQueryList(nestedMainSqlListMerge, cacheMap, (sql, list) -> NestedFieldWriter.executeEsTemplateUpdate(bulkRequestList, esTemplate, sql, list));
            }

            List<MergeJdbcTemplateSQL<DependentSQL>> joinForeignKeyMerge = this.joinForeignKeyMerge = joinForeignKey.parentSqlList.isEmpty() ?
                    Collections.emptyList() : MergeJdbcTemplateSQL.merge(joinForeignKey.parentSqlList, chunkSize);
            if (!joinForeignKeyMerge.isEmpty()) {
                Map<String, List<Map<String, Object>>> parentGetterMap = MergeJdbcTemplateSQL.toMap(joinForeignKeyMerge, e -> id(e.getDependent(), MergeJdbcTemplateSQL.argsToString(e.getArgs())), cacheMap);
                List<MergeJdbcTemplateSQL<DependentSQL>> childMergeSqlList = MergeJdbcTemplateSQL.merge(joinForeignKey.childSqlSet, chunkSize, false);
                Set<String> visited = new HashSet<>(parentGetterMap.size());
                for (MergeJdbcTemplateSQL<DependentSQL> templateSQL : childMergeSqlList) {
                    templateSQL.executeQueryStream(streamChunkSize, DependentSQL::getDependent, chunk -> {
                        String dmlUniqueColumnKey = id(chunk.source, chunk.uniqueColumnKey);
                        List<Object> pkList = chunk.rowListFirst(visited, dmlUniqueColumnKey);
                        if (!pkList.isEmpty()) {
                            List<Map<String, Object>> parentData = parentGetterMap.get(dmlUniqueColumnKey);
                            SchemaItem schemaItem = chunk.source.getSchemaItem();
                            NestedFieldWriter.executeEsTemplateUpdate(bulkRequestList, esTemplate, pkList, schemaItem, parentData);
                        }
                    });
                    bulkRequestList.commit(esTemplate);
                }
            }

            bulkRequestList.commit(esTemplate);
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
        for (SqlDependent sqlDependent : updateDmlList) {
            Dml dml = sqlDependent.getDml();
            if (ESSyncUtil.isEmpty(dml.getPkNames())) {
                continue;
            }

            if (sqlDependent.isJoinByMainTablePrimaryKey()) {
                Set<DependentSQL> nestedMainSqlList = convertDependentSQL(sqlDependent.getNestedSlaveTableList(dml.getTable()), sqlDependent, cacheMap, chunkSize);
                if (nestedMainSqlList.isEmpty()) {
                    continue;
                }
                joinBySlaveTable.nestedMainSqlList.addAll(nestedMainSqlList);
            } else {
                Set<DependentSQL> childSqlSet = convertDependentSQLJoinByParentSlaveTable(sqlDependent.getNestedSlaveTableList(dml.getTable()), sqlDependent, cacheMap, chunkSize);
                if (childSqlSet.isEmpty()) {
                    continue;
                }
                Set<DependentSQL> parentSqlList = convertDependentSQL(sqlDependent.getNestedSlaveTableList(dml.getTable()), sqlDependent, cacheMap, chunkSize);
                if (parentSqlList.isEmpty()) {
                    continue;
                }
                joinForeignKey.childSqlSet.addAll(childSqlSet);
                joinForeignKey.parentSqlList.addAll(parentSqlList);
            }
        }
    }

    private Set<DependentSQL> convertDependentSQL(List<SchemaItem.TableItem> nestedSlaveTableList, SqlDependent sqlDependent, CacheMap cacheMap, int chunkSize) {
        Set<DependentSQL> byChildrenSqlSet = new LinkedHashSet<>();
        for (SchemaItem.TableItem tableItem : nestedSlaveTableList) {
            SQL nestedChildrenSql = convertNestedSlaveTableSQL(tableItem, sqlDependent);
            byChildrenSqlSet.add(new DependentSQL(nestedChildrenSql, sqlDependent, null));
        }

        String fullSql = sqlDependent.getSchemaItem().getObjectField().getParamSql().getFullSql(false);
        Set<DependentSQL> byMainSqlList = new LinkedHashSet<>();
        MergeJdbcTemplateSQL.executeQueryList(MergeJdbcTemplateSQL.merge(byChildrenSqlSet, chunkSize), cacheMap, (sql, changeRowList) -> {
            for (Map<String, Object> changeRow : changeRowList) {
                byMainSqlList.add(new DependentSQL(SQL.convertToSql(fullSql, changeRow), sqlDependent, sqlDependent.getSchemaItem().getGroupByIdColumns()));
            }
        });
        return byMainSqlList;
    }

    private Set<DependentSQL> convertDependentSQLJoinByParentSlaveTable(List<SchemaItem.TableItem> nestedSlaveTableList, SqlDependent sqlDependent,
                                                                        CacheMap cacheMap, int chunkSize) {
        Set<DependentSQL> byChildrenSqlSet = new LinkedHashSet<>();
        for (SchemaItem.TableItem tableItem : nestedSlaveTableList) {
            SQL nestedChildrenSql = convertNestedSlaveTableSQL(tableItem, sqlDependent);
            byChildrenSqlSet.add(new DependentSQL(nestedChildrenSql, sqlDependent, null));
        }

        String fullSql = getJoinByParentSlaveTableSql(sqlDependent);
        Set<DependentSQL> byMainSqlList = new LinkedHashSet<>();
        MergeJdbcTemplateSQL.executeQueryList(MergeJdbcTemplateSQL.merge(byChildrenSqlSet, chunkSize), cacheMap, (sql, changeRowList) -> {
            for (Map<String, Object> changeRow : changeRowList) {
                byMainSqlList.add(new DependentSQL(SQL.convertToSql(fullSql, changeRow), sqlDependent, sqlDependent.getSchemaItem().getGroupByIdColumns()));
            }
        });
        return byMainSqlList;
    }

    private String getJoinByParentSlaveTableSql(SqlDependent sqlDependent) {
        StringBuilder condition = new StringBuilder();
        String and = " AND ";

        List<SqlParser.BinaryOpExpr> joinVarColumnList = sqlDependent.getSchemaItem().getOnMainTableChangeWhereSqlVarColumnList();
        for (SqlParser.BinaryOpExpr varColumn : joinVarColumnList) {
            if (varColumn.isPlaceholder()) {
                ESSyncUtil.appendConditionByExpr(condition, SQL.wrapPlaceholder(varColumn.getName()), sqlDependent.getIndexMainTable().getAlias(), SQL.removeWrapPlaceholder(varColumn.getValue()), and);
            }
        }
        int len = condition.length();
        condition.delete(len - and.length(), len);

        SchemaItem indexMainTable = sqlDependent.getIndexMainTable().getSchemaItem();
        SchemaItem.FieldItem idField = indexMainTable.getIdField();

        String changedSelect = SqlParser.changeSelect(indexMainTable.sql(),
                Collections.singletonMap(idField.getOwner(), Collections.singletonList(idField.getColumnName())),
                true);
        return changedSelect + " WHERE " + condition + " ";
    }

    private SQL convertNestedSlaveTableSQL(SchemaItem.TableItem tableItem, SqlDependent sqlDependent) {
        Dml dml = sqlDependent.getDml();
        StringBuilder condition = new StringBuilder();
        String and = " AND ";
        for (String pkName : dml.getPkNames()) {
            ESSyncUtil.appendConditionByExpr(condition, SQL.wrapPlaceholder(pkName), tableItem.getAlias(), pkName, and);
        }
        int len = condition.length();
        condition.delete(len - and.length(), len);

        String sql1 = sqlDependent.getSchemaItem().sql() + " WHERE " + condition + " ";

        Map<String, List<String>> columnList = sqlDependent.getSchemaItem().getOnSlaveTableChangeWhereSqlColumnList();
        String sql2 = SqlParser.changeSelect(sql1, columnList, true);
        return SQL.convertToSql(sql2, sqlDependent.getMergeAfterDataMap());
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
