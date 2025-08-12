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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Nested字段的主表发生变化时，对应的处理逻辑
 * 例如下配置：
 * corp就是主表，它发生变化后会触发逻辑。
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
class NestedMainJoinTableRunnable extends CompletableFuture<Void> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(NestedMainJoinTableRunnable.class);
    private final List<SqlDependent> dmlList;
    private final DefaultESTemplate esTemplate;
    private final int maxIdInCount;
    private final int streamChunkSize;
    private final Timestamp maxTimestamp;
    private final NestedMainJoinTableRunnable oldRun;
    private final NestedMainJoinTableRunnable newRun;
    private final ESTemplate.BulkRequestList parentBulkRequestList;
    private final ESTemplate.BulkRequestList bulkRequestList;
    private final ESTemplate.CommitListener commitListener;

    NestedMainJoinTableRunnable(List<SqlDependent> dmlList, DefaultESTemplate esTemplate,
                                ESTemplate.BulkRequestList bulkRequestList,
                                ESTemplate.CommitListener commitListener,
                                int maxIdInCount, int streamChunkSize, Timestamp maxTimestamp) {
        this(dmlList, esTemplate, bulkRequestList, commitListener, maxIdInCount, streamChunkSize, maxTimestamp, null, null);
    }

    NestedMainJoinTableRunnable(List<SqlDependent> dmlList, DefaultESTemplate esTemplate,
                                ESTemplate.BulkRequestList bulkRequestList,
                                ESTemplate.CommitListener commitListener,
                                int maxIdInCount, int streamChunkSize, Timestamp maxTimestamp,
                                NestedMainJoinTableRunnable oldRun,
                                NestedMainJoinTableRunnable newRun) {
        this.dmlList = dmlList;
        this.esTemplate = esTemplate;
        this.commitListener = commitListener;
        this.maxIdInCount = maxIdInCount;
        this.parentBulkRequestList = bulkRequestList;
        this.bulkRequestList = bulkRequestList.fork(BulkPriorityEnum.LOW);
        this.streamChunkSize = streamChunkSize;
        this.maxTimestamp = maxTimestamp;
        this.oldRun = oldRun;
        this.newRun = newRun;
    }

    static class Executor {
        private final AtomicInteger idIncr = new AtomicInteger();
        private final java.util.concurrent.Executor executor;
        private final Map<Integer, NestedMainJoinTableRunnable> runnableMap = new ConcurrentHashMap<>();

        Executor(java.util.concurrent.Executor executor) {
            this.executor = executor;
        }

        void execute(NestedMainJoinTableRunnable runnable) {
            trace(runnable);
            executor.execute(runnable);
        }

        void trace(NestedMainJoinTableRunnable runnable) {
            int id = idIncr.incrementAndGet();
            runnableMap.put(id, runnable);
            runnable.whenComplete((unused, throwable) -> runnableMap.remove(id));
        }

        EsSyncRunnableStatus status() {
            EsSyncRunnableStatus status = new EsSyncRunnableStatus();
            for (NestedMainJoinTableRunnable value : runnableMap.values()) {
                EsSyncRunnableStatus.Row row = new EsSyncRunnableStatus.Row();
                row.setTotal(value.dmlList.size());
                row.setCurr(value.bulkRequestList.commitRequests());
                row.setBinlogTimestamp(String.valueOf(value.maxTimestamp));
                row.setInfo(value.dmlList.stream().map(SqlDependent::toString).collect(Collectors.toList()));
                status.getRows().add(row);
            }
            return status;
        }
    }

    static NestedMainJoinTableRunnable merge(NestedMainJoinTableRunnable oldRun, NestedMainJoinTableRunnable newRun) {
        List<SqlDependent> dmlList = new ArrayList<>(oldRun.dmlList.size() + newRun.dmlList.size());
        dmlList.addAll(oldRun.dmlList);
        dmlList.addAll(newRun.dmlList);
        return new NestedMainJoinTableRunnable(dmlList, oldRun.esTemplate,
                oldRun.parentBulkRequestList.fork(newRun.parentBulkRequestList),
                ESTemplate.merge(oldRun.commitListener, newRun.commitListener),
                oldRun.maxIdInCount, oldRun.streamChunkSize, oldRun.maxTimestamp,
                oldRun, newRun);
    }

    private static DependentSQL convertParentSql(SqlDependent sqlDependent, Function<SqlDependent, Map<String, Object>> rowGetter) {
        String fullSql = sqlDependent.getSchemaItem().getObjectField().getParamSql().getFullSql(false);
        return new DependentSQL(SQL.convertToSql(fullSql, rowGetter.apply(sqlDependent)), sqlDependent, sqlDependent.getSchemaItem().getGroupByIdColumns());
    }

    private static DependentSQL convertChildrenSQL(SqlDependent sqlDependent, Function<SqlDependent, Map<String, Object>> rowGetter) {
        SchemaItem.TableItem tableItem = sqlDependent.getIndexMainTable();

        Dml dml = sqlDependent.getDml();
        StringBuilder condition = new StringBuilder();
        String and = " AND ";

        if (sqlDependent.getSchemaItem().getObjectField().getType().isSingleJoinType()) {
            Set<String> pkNames = sqlDependent.getSchemaItem().getOnMainTableChangeWhereSqlVarList();
            List<String> dmlPkNames = dml.getPkNames();
            String pkName = pkNames.iterator().next();
            ESSyncUtil.appendConditionByExpr(condition, SQL.wrapPlaceholder(dmlPkNames.get(0)), tableItem.getAlias(), pkName, and);
        } else {
            String joinTableColumnName = sqlDependent.getSchemaItem().getObjectField().getParamSql().getJoinTableColumnName();
            ESSyncUtil.appendConditionByExpr(condition, SQL.wrapPlaceholder(joinTableColumnName), tableItem.getAlias(), joinTableColumnName, and);
        }

        int len = condition.length();
        condition.delete(len - and.length(), len);

        String sql1 = tableItem.getSchemaItem().sql() + " WHERE " + condition + " ";

        Map<String, List<String>> columnList = Collections.singletonMap(tableItem.getAlias(), dml.getPkNames());
        String sql2 = SqlParser.changeSelect(sql1, columnList, false);

        SQL sql = SQL.convertToSql(sql2, rowGetter.apply(sqlDependent));
        return new DependentSQL(sql, sqlDependent, null);
    }

    @Override
    public void run() {
        List<SqlDependent> sqlDependentList = dmlList.stream().filter(e -> !ESSyncUtil.isEmpty(e.getDml().getPkNames())).collect(Collectors.toList());
        if (sqlDependentList.isEmpty()) {
            return;
        }
        AtomicInteger childrenCounter = new AtomicInteger();
        try {
            // 这种一条sql：update corp_region set corp_id = 2 where id = xx and corp_id=1
            // 更新 corp_id = 2 影响的数据，或insert语句，delete语句
            executeRowChange(sqlDependentList, childrenCounter, bulkRequestList, false);
            // 更新 corp_id = 1 影响的数据
            executeRowChange(sqlDependentList, childrenCounter, bulkRequestList, true);
            complete(null);
        } catch (Exception e) {
            log.info("NestedMainJoinTable={}ms, rowCount={}, ts={}, dependentList={}, error={}",
                    System.currentTimeMillis() - maxTimestamp.getTime(),
                    childrenCounter.intValue(),
                    maxTimestamp, sqlDependentList, e.toString(), e);
            completeExceptionally(e);
            throw e;
        }
    }

    private void executeRowChange(List<SqlDependent> sqlDependentList, AtomicInteger childrenCounter, ESTemplate.BulkRequestList bulkRequestList, boolean before) {
        List<DependentSQL> parentSqlList = new ArrayList<>(sqlDependentList.size());
        List<DependentSQL> childrenSqlList = new ArrayList<>(sqlDependentList.size());
        for (SqlDependent sqlDependent : sqlDependentList) {
            if (before) {
                if (sqlDependent.getDml().isTypeUpdate()) {
                    parentSqlList.add(convertParentSql(sqlDependent, SqlDependent::getMergeBeforeDataMap));
                    childrenSqlList.add(convertChildrenSQL(sqlDependent, SqlDependent::getMergeBeforeDataMap));
                }
            } else {
                parentSqlList.add(convertParentSql(sqlDependent, SqlDependent::getMergeAfterDataMap));
                childrenSqlList.add(convertChildrenSQL(sqlDependent, SqlDependent::getMergeAfterDataMap));
            }
        }

        List<MergeJdbcTemplateSQL<DependentSQL>> mergeNestedMainSqlList = MergeJdbcTemplateSQL.merge(parentSqlList, maxIdInCount);
        List<MergeJdbcTemplateSQL<DependentSQL>> childrenMergeSqlList = MergeJdbcTemplateSQL.merge(childrenSqlList, maxIdInCount);

        Map<SqlDependent, List<Map<String, Object>>> parentGetterMap = MergeJdbcTemplateSQL.toMap(mergeNestedMainSqlList, DependentSQL::getDependent);
        for (MergeJdbcTemplateSQL<DependentSQL> children : childrenMergeSqlList) {
            children.executeQueryStream(streamChunkSize, DependentSQL::getDependent, (chunk) -> {
                childrenCounter.addAndGet(chunk.rowList.size());
                SchemaItem schemaItem = chunk.source.getSchemaItem();
                List<Map<String, Object>> parentList = parentGetterMap.get(chunk.source);
                NestedFieldWriter.executeEsTemplateUpdate(bulkRequestList, esTemplate, chunk.rowListFirst(), schemaItem, parentList);
            });
            bulkRequestList.commit(esTemplate);
        }

        log.info("NestedMainJoinTable={}ms, rowCount={}, ts={}, dependentList={}, changeSql={}",
                System.currentTimeMillis() - maxTimestamp.getTime(),
                childrenCounter.intValue(),
                maxTimestamp, sqlDependentList, childrenMergeSqlList);

        bulkRequestList.commit(esTemplate);
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
}
