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

    NestedMainJoinTableRunnable(List<Dependent> dmlList, ES7xTemplate es7xTemplate, int maxIdInCount, int streamChunkSize, Timestamp maxTimestamp) {
        this.dmlList = dmlList;
        this.es7xTemplate = es7xTemplate;
        this.maxIdInCount = maxIdInCount;
        this.streamChunkSize = streamChunkSize;
        this.maxTimestamp = maxTimestamp;
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

        if (dependent.getSchemaItem().getObjectField().getType() == ESSyncConfig.ObjectField.Type.OBJECT_SQL) {
            Set<String> pkNames = SQL.convertToSql(dependent.getSchemaItem().getObjectField().getOnParentChangeWhereSql(), Collections.emptyMap()).getArgsMap().keySet();
            List<String> dmlPkNames = dml.getPkNames();
            String pkName = pkNames.iterator().next();
            ESSyncUtil.appendConditionByExpr(condition, "#{" + dmlPkNames.get(0) + "}", tableItem.getAlias(), pkName, and);
        } else {
            String parentDocumentId = dependent.getSchemaItem().getObjectField().getParentDocumentId();
            ESSyncUtil.appendConditionByExpr(condition, "#{" + parentDocumentId + "}", tableItem.getAlias(), parentDocumentId, and);
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
            ESTemplate.BulkRequestList bulkRequestList = es7xTemplate.newBulkRequestList(BulkPriorityEnum.LOW);

            Map<Dependent, List<Map<String, Object>>> parentGetterMap = MergeJdbcTemplateSQL.toMap(mergeNestedMainSqlList, DependentSQL::getDependent);
            for (MergeJdbcTemplateSQL<DependentSQL> children : childrenMergeSqlList) {
                children.executeQueryStream(streamChunkSize, DependentSQL::getDependent,(chunk) -> {
                    childrenCounter.addAndGet(chunk.rowList.size());
                    SchemaItem schemaItem = chunk.source.getSchemaItem();
                    List<Map<String, Object>> parentList = parentGetterMap.get(chunk.source);
                    for (Map<String, Object> row : chunk.rowList) {
                        if (row.isEmpty()) {
                            continue;
                        }
                        Object pkValue = row.values().iterator().next();
                        NestedFieldWriter.executeEsTemplateUpdate(bulkRequestList, es7xTemplate, pkValue, schemaItem, parentList);
                    }
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

}
