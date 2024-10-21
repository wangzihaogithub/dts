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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

class NestedMainJoinTableRunnable extends CompletableFuture<Void> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(NestedMainJoinTableRunnable.class);
    private final List<Dependent> dmlList;
    private final ES7xTemplate es7xTemplate;
    private final int maxIdInCount;
    private final int streamChunkSize;
    private final Timestamp maxTimestamp;
    private final NestedMainJoinTableRunnable oldRun;
    private final NestedMainJoinTableRunnable newRun;
    private final ESTemplate.BulkRequestList bulkRequestList;
    private final ESTemplate.CommitListener commitListener;

    NestedMainJoinTableRunnable(List<Dependent> dmlList, ES7xTemplate es7xTemplate,
                                ESTemplate.BulkRequestList bulkRequestList,
                                ESTemplate.CommitListener commitListener,
                                int maxIdInCount, int streamChunkSize, Timestamp maxTimestamp) {
        this(dmlList, es7xTemplate, bulkRequestList, commitListener, maxIdInCount, streamChunkSize, maxTimestamp, null, null);
    }

    NestedMainJoinTableRunnable(List<Dependent> dmlList, ES7xTemplate es7xTemplate,
                                ESTemplate.BulkRequestList bulkRequestList,
                                ESTemplate.CommitListener commitListener,
                                int maxIdInCount, int streamChunkSize, Timestamp maxTimestamp,
                                NestedMainJoinTableRunnable oldRun,
                                NestedMainJoinTableRunnable newRun) {
        this.dmlList = dmlList;
        this.es7xTemplate = es7xTemplate;
        this.commitListener = commitListener;
        this.maxIdInCount = maxIdInCount;
        this.bulkRequestList = bulkRequestList;
        this.streamChunkSize = streamChunkSize;
        this.maxTimestamp = maxTimestamp;
        this.oldRun = oldRun;
        this.newRun = newRun;
    }

    static NestedMainJoinTableRunnable merge(NestedMainJoinTableRunnable oldRun, NestedMainJoinTableRunnable newRun) {
        List<Dependent> dmlList = new ArrayList<>(oldRun.dmlList.size() + newRun.dmlList.size());
        dmlList.addAll(oldRun.dmlList);
        dmlList.addAll(newRun.dmlList);
        return new NestedMainJoinTableRunnable(dmlList, oldRun.es7xTemplate,
                oldRun.bulkRequestList.fork(newRun.bulkRequestList),
                ESTemplate.merge(oldRun.commitListener, newRun.commitListener),
                oldRun.maxIdInCount, oldRun.streamChunkSize, oldRun.maxTimestamp,
                oldRun, newRun);
    }

    private static DependentSQL convertParentSql(Dependent dependent, Function<Dependent, Map<String, Object>> rowGetter) {
        String fullSql = dependent.getSchemaItem().getObjectField().getParamSql().getFullSql(false);
        return new DependentSQL(SQL.convertToSql(fullSql, rowGetter.apply(dependent)), dependent, dependent.getSchemaItem().getGroupByIdColumns());
    }

    private static DependentSQL convertChildrenSQL(Dependent dependent, Function<Dependent, Map<String, Object>> rowGetter) {
        SchemaItem.TableItem tableItem = dependent.getIndexMainTable();

        Dml dml = dependent.getDml();
        StringBuilder condition = new StringBuilder();
        String and = " AND ";

        if (dependent.getSchemaItem().getObjectField().getType().isSingleJoinType()) {
            Set<String> pkNames = dependent.getSchemaItem().getOnMainTableChangeWhereSqlVarList();
            List<String> dmlPkNames = dml.getPkNames();
            String pkName = pkNames.iterator().next();
            ESSyncUtil.appendConditionByExpr(condition, SQL.wrapPlaceholder(dmlPkNames.get(0)), tableItem.getAlias(), pkName, and);
        } else {
            String joinTableColumnName = dependent.getSchemaItem().getObjectField().getParamSql().getJoinTableColumnName();
            ESSyncUtil.appendConditionByExpr(condition, SQL.wrapPlaceholder(joinTableColumnName), tableItem.getAlias(), joinTableColumnName, and);
        }

        int len = condition.length();
        condition.delete(len - and.length(), len);

        String sql1 = tableItem.getSchemaItem().sql() + " WHERE " + condition + " ";

        Map<String, List<String>> columnList = Collections.singletonMap(tableItem.getAlias(), dml.getPkNames());
        String sql2 = SqlParser.changeSelect(sql1, columnList, false);

        SQL sql = SQL.convertToSql(sql2, rowGetter.apply(dependent));
        return new DependentSQL(sql, dependent, null);
    }

    @Override
    public void run() {
        List<Dependent> dependentList = dmlList.stream().filter(e -> !ESSyncUtil.isEmpty(e.getDml().getPkNames())).collect(Collectors.toList());
        if (dependentList.isEmpty()) {
            return;
        }
        AtomicInteger childrenCounter = new AtomicInteger();
        ESTemplate.BulkRequestList bulkRequestList = this.bulkRequestList.fork(BulkPriorityEnum.LOW);
        try {
            // 这种一条sql：update corp_region set corp_id = 2 where id = xx and corp_id=1
            // 更新 corp_id = 2 影响的数据，或insert语句，delete语句
            executeRowChange(dependentList, childrenCounter, bulkRequestList, false);
            // 更新 corp_id = 1 影响的数据
            executeRowChange(dependentList, childrenCounter, bulkRequestList, true);
            complete(null);
        } catch (Exception e) {
            log.info("NestedMainJoinTable={}ms, rowCount={}, ts={}, dependentList={}, error={}",
                    System.currentTimeMillis() - maxTimestamp.getTime(),
                    childrenCounter.intValue(),
                    maxTimestamp, dependentList, e.toString(), e);
            completeExceptionally(e);
            throw e;
        }
    }

    private void executeRowChange(List<Dependent> dependentList, AtomicInteger childrenCounter, ESTemplate.BulkRequestList bulkRequestList, boolean before) {
        List<DependentSQL> parentSqlList = new ArrayList<>(dependentList.size());
        List<DependentSQL> childrenSqlList = new ArrayList<>(dependentList.size());
        for (Dependent dependent : dependentList) {
            if (before) {
                if (dependent.getDml().isTypeUpdate()) {
                    parentSqlList.add(convertParentSql(dependent, Dependent::getMergeBeforeDataMap));
                    childrenSqlList.add(convertChildrenSQL(dependent, Dependent::getMergeBeforeDataMap));
                }
            } else {
                parentSqlList.add(convertParentSql(dependent, Dependent::getMergeAfterDataMap));
                childrenSqlList.add(convertChildrenSQL(dependent, Dependent::getMergeAfterDataMap));
            }
        }

        List<MergeJdbcTemplateSQL<DependentSQL>> mergeNestedMainSqlList = MergeJdbcTemplateSQL.merge(parentSqlList, maxIdInCount);
        List<MergeJdbcTemplateSQL<DependentSQL>> childrenMergeSqlList = MergeJdbcTemplateSQL.merge(childrenSqlList, maxIdInCount);

        Map<Dependent, List<Map<String, Object>>> parentGetterMap = MergeJdbcTemplateSQL.toMap(mergeNestedMainSqlList, DependentSQL::getDependent);
        for (MergeJdbcTemplateSQL<DependentSQL> children : childrenMergeSqlList) {
            children.executeQueryStream(streamChunkSize, DependentSQL::getDependent, (chunk) -> {
                childrenCounter.addAndGet(chunk.rowList.size());
                SchemaItem schemaItem = chunk.source.getSchemaItem();
                List<Map<String, Object>> parentList = parentGetterMap.get(chunk.source);
                NestedFieldWriter.executeEsTemplateUpdate(bulkRequestList, es7xTemplate, chunk.rowListFirst(), schemaItem, parentList);
            });
            bulkRequestList.commit(es7xTemplate);
        }

        log.info("NestedMainJoinTable={}ms, rowCount={}, ts={}, dependentList={}, changeSql={}",
                System.currentTimeMillis() - maxTimestamp.getTime(),
                childrenCounter.intValue(),
                maxTimestamp, dependentList, childrenMergeSqlList);

        bulkRequestList.commit(es7xTemplate);
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
