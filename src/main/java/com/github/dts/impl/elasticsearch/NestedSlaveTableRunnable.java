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

/**
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
 */
class NestedSlaveTableRunnable extends CompletableFuture<Void> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(NestedSlaveTableRunnable.class);
    private final List<SqlDependent> updateDmlList;
    private final DefaultESTemplate esTemplate;
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
        this.bulkRequestList = bulkRequestList;
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
                oldRun.bulkRequestList.fork(newRun.bulkRequestList),
                ESTemplate.merge(oldRun.commitListener, newRun.commitListener),
                oldRun.cacheMap, oldRun.timestamp, oldRun.chunkSize, oldRun.streamChunkSize,
                oldRun, newRun);
    }

    static String id(SqlDependent id, String key) {
        return id.dmlKey() + "_" + key;
    }

    @Override
    public void run() {
        parseSql(chunkSize, cacheMap);
        try {
            ESTemplate.BulkRequestList bulkRequestList = this.bulkRequestList.fork(BulkPriorityEnum.LOW);
            if (!joinBySlaveTable.nestedMainSqlList.isEmpty()) {
                List<MergeJdbcTemplateSQL<DependentSQL>> merge = MergeJdbcTemplateSQL.merge(joinBySlaveTable.nestedMainSqlList, chunkSize);
                MergeJdbcTemplateSQL.executeQueryList(merge, cacheMap, (sql, list) -> NestedFieldWriter.executeEsTemplateUpdate(bulkRequestList, esTemplate, sql, list));
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
