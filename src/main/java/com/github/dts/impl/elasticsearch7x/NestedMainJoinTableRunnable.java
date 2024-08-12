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

    private static DependentSQL convertParentSql(Dependent dependent) {
        String fullSql = dependent.getSchemaItem().getObjectField().getFullSql(false);
        return new DependentSQL(SQL.convertToSql(fullSql, dependent.getMergeDataMap()), dependent, dependent.getSchemaItem().getGroupByIdColumns());
    }

    private static DependentSQL convertChildrenSQL(Dependent dependent) {
        SchemaItem.TableItem tableItem = dependent.getIndexMainTable();

        Dml dml = dependent.getDml();
        StringBuilder condition = new StringBuilder();
        String and = " AND ";

        if (dependent.getSchemaItem().getObjectField().getType().isSingleJoinType()) {
            Set<String> pkNames = SQL.convertToSql(dependent.getSchemaItem().getObjectField().getOnParentChangeWhereSql(), Collections.emptyMap()).getArgsMap().keySet();
            List<String> dmlPkNames = dml.getPkNames();
            String pkName = pkNames.iterator().next();
            ESSyncUtil.appendConditionByExpr(condition, SQL.wrapPlaceholder(dmlPkNames.get(0)), tableItem.getAlias(), pkName, and);
        } else {
            String parentDocumentId = dependent.getSchemaItem().getObjectField().getParentDocumentId();
            ESSyncUtil.appendConditionByExpr(condition, SQL.wrapPlaceholder(parentDocumentId), tableItem.getAlias(), parentDocumentId, and);
        }

        int len = condition.length();
        condition.delete(len - and.length(), len);

        String sql1 = tableItem.getSchemaItem().sql() + " WHERE " + condition + " ";

        Map<String, List<String>> columnList = Collections.singletonMap(tableItem.getAlias(), dml.getPkNames());
        String sql2 = SqlParser.changeSelect(sql1, columnList, false);

        SQL sql = SQL.convertToSql(sql2, dependent.getMergeDataMap());
        return new DependentSQL(sql, dependent, null);
    }

    @Override
    public void run() {
        List<Dependent> dependentList = dmlList.stream().filter(e -> !ESSyncUtil.isEmpty(e.getDml().getPkNames())).collect(Collectors.toList());
        if (dependentList.isEmpty()) {
            return;
        }
        AtomicInteger childrenCounter = new AtomicInteger();
        try {
            List<DependentSQL> parentSqlList = new ArrayList<>(dependentList.size());
            List<DependentSQL> childrenSqlList = new ArrayList<>(dependentList.size());
            for (Dependent dependent : dependentList) {
                parentSqlList.add(convertParentSql(dependent));
                childrenSqlList.add(convertChildrenSQL(dependent));
            }

            List<MergeJdbcTemplateSQL<DependentSQL>> mergeNestedMainSqlList = MergeJdbcTemplateSQL.merge(parentSqlList, maxIdInCount);
            List<MergeJdbcTemplateSQL<DependentSQL>> childrenMergeSqlList = MergeJdbcTemplateSQL.merge(childrenSqlList, maxIdInCount);
            ESTemplate.BulkRequestList bulkRequestList = this.bulkRequestList.fork(BulkPriorityEnum.LOW);

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
