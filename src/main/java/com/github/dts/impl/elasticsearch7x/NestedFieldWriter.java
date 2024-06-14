package com.github.dts.impl.elasticsearch7x;

import com.github.dts.util.*;
import com.google.common.collect.Lists;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;
import org.springframework.util.LinkedMultiValueMap;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * 对象字段监听
 *
 * @author acer01
 */
public class NestedFieldWriter {
    private static final ThreadLocal<Map<String, Object>> MERGE_DATA_MAP_THREAD_LOCAL = ThreadLocal.withInitial(HashMap::new);
    private static final ThreadLocal<Queue<Placeholder>> PLACEHOLDER_QUEUE_THREAD_LOCAL = ThreadLocal.withInitial(ArrayDeque::new);
    private static final String PLACEHOLDER_BEGIN = "#{";
    private static final String PLACEHOLDER_END = "}";
    private final Map<String, List<SchemaItem>> schemaItemMap;
    private final ESTemplate esTemplate;
    private final CacheMap cacheMap;
    private final ExecutorService listenerExecutor;
    private final int threads;

    public NestedFieldWriter(int nestedFieldThreads, Map<String, ESSyncConfig> map, ESTemplate esTemplate, CacheMap cacheMap) {
        this.schemaItemMap = toListenerMap(map);
        this.esTemplate = esTemplate;
        this.cacheMap = cacheMap;
        this.threads = nestedFieldThreads;
        this.listenerExecutor = Util.newFixedThreadPool(
                nestedFieldThreads,
                60_000L, "ES-NestedFieldWriter", true);
    }

    /**
     * 循环所有依赖影响的字段
     *
     * @param dml  dml数据
     * @param call 每一次发现有依赖字段的回调方法
     */
    private static void forEachSqlField(List<SchemaItem> schemaItemList, Dml dml, BiFunction<Integer, SchemaItem, Boolean> call) {
        //当前表所依赖的所有sql
        for (SchemaItem schemaItem : schemaItemList) {
            //获取当前表在这个sql中的别名
            for (String alias : schemaItem.getTableItemAliases(dml.getTable())) {
                //批量多条DML
                int dmlIndex = 0;
                if (!CollectionUtils.isEmpty(dml.getOld())) {
                    for (Map<String, Object> old : dml.getOld()) {
                        //DML的字段
                        for (String field : old.keySet()) {
                            //寻找依赖字段
                            if (!schemaItem.getFields().containsKey(alias + "." + field)) {
                                continue;
                            }
                            //如果不需要往下继续执行
                            boolean isNext = call.apply(dmlIndex, schemaItem);
                            if (!isNext) {
                                return;
                            }
                        }
                        dmlIndex++;
                    }
                } else {
                    boolean isNext = call.apply(dmlIndex, schemaItem);
                    if (!isNext) {
                        return;
                    }
                }

            }
        }
    }

    /**
     * 初始化监听表与sql的关系
     *
     * @return 表与sql的关系
     */
    private static Map<String, List<SchemaItem>> toListenerMap(Map<String, ESSyncConfig> esSyncConfigMap) {
        LinkedMultiValueMap<String, SchemaItem> schemaItemMap = new LinkedMultiValueMap<>();
        esSyncConfigMap.values().stream()
                .map(ESSyncConfig::getEsMapping)
                .map(ESSyncConfig.ESMapping::getObjFields)
                .map(Map::values)
                .flatMap(Collection::stream)
                .map(ESSyncConfig.ObjectField::getSchemaItem)
                .filter(Objects::nonNull)
                .forEach(schemaItem -> {
                    for (String tableName : schemaItem.getTableItemAliases().keySet()) {
                        schemaItemMap.add(tableName, schemaItem);
                    }
                });
        return Collections.unmodifiableMap(schemaItemMap);
    }

    /**
     * 将sql表达式与参数 转换为JDBC所需的sql对象
     *
     * @param expressionsSql sql表达式
     * @param args           参数
     * @return JDBC所需的sql对象
     */
    private static SQL convertToSql(String expressionsSql, Map<String, Object> args, SchemaItem schemaItem) {
        Queue<Placeholder> placeholderQueue = getPlaceholderQueue(expressionsSql);

        List<Object> argsList = new ArrayList<>();
        StringBuilder sqlBuffer = new StringBuilder(expressionsSql);
        Placeholder placeholder;
        while ((placeholder = placeholderQueue.poll()) != null) {
            int offset = expressionsSql.length() - sqlBuffer.length();
            sqlBuffer.replace(
                    placeholder.getBeginIndex() - PLACEHOLDER_BEGIN.length() - offset,
                    placeholder.getEndIndex() + PLACEHOLDER_END.length() - offset,
                    "?");
            Object value = args.get(placeholder.getKey());
            argsList.add(value);
        }
        return new SQL(sqlBuffer.toString(), argsList.toArray(), args, schemaItem);
    }

    /**
     * 获取占位符
     *
     * @param str 表达式
     * @return 多个占位符
     */
    private static Queue<Placeholder> getPlaceholderQueue(String str) {
        int charAt = 0;

        Queue<Placeholder> keys = PLACEHOLDER_QUEUE_THREAD_LOCAL.get();
        keys.clear();
        while (true) {
            charAt = str.indexOf(PLACEHOLDER_BEGIN, charAt);
            if (charAt == -1) {
                return keys;
            }
            charAt = charAt + PLACEHOLDER_BEGIN.length();
            keys.add(new Placeholder(str, charAt, str.indexOf(PLACEHOLDER_END, charAt)));
        }
    }

    /**
     * 转换类型
     *
     * @param esMapping     es映射关系
     * @param dmlDataMap    DML数据
     * @param theConvertMap 需要转换的数据
     */
    private static void convertValueType(ESSyncConfig.ESMapping esMapping,
                                         Map<String, Object> dmlDataMap,
                                         Map<String, Object> theConvertMap, ESTemplate esTemplate) {
        for (Map.Entry<String, Object> entry : theConvertMap.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();
            Object newValue = esTemplate.getValFromValue(
                    esMapping, fieldValue,
                    dmlDataMap, fieldName);
            entry.setValue(newValue);
        }
    }

    public void write(List<Dml> dmls, ESTemplate.BulkRequestList bulkRequestList) {
        Set<SQL> sqlList = new LinkedHashSet<>();
        for (Dml dml : dmls) {
            if (Boolean.TRUE.equals(dml.getIsDdl())) {
                continue;
            }
            sqlList.addAll(convertToSql(dml));
        }
        if (sqlList.isEmpty()) {
            return;
        }
        List<List<SQL>> partition = Lists.partition(new ArrayList<>(sqlList), Math.max(5, (sqlList.size() + 1) / threads));
        List<Future> futures = partition.stream().map(e -> listenerExecutor.submit(() -> writeSql(e, bulkRequestList))).collect(Collectors.toList());
        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                Util.sneakyThrows(e);
            }
        }
    }

    private void writeSql(List<SQL> sqlList, ESTemplate.BulkRequestList bulkRequestList) {
        for (SQL sql : sqlList) {
            ESSyncConfig.ObjectField objectField = sql.schemaItem.getObjectField();
            ESSyncConfig.ESMapping esMapping = objectField.getEsMapping();
            ESSyncConfig esSyncConfig = esMapping.getConfig();
            JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(esSyncConfig.getDataSourceKey());
            Map<String, Object> mergeDataMap = sql.argsMap;
            switch (objectField.getType()) {
                case ARRAY_SQL: {
                    String cacheKey = "ARRAY_SQL_" + sql.getExprSql() + "_" + Arrays.toString(sql.getArgs());
                    List<Map<String, Object>> resultList = cacheMap.cacheComputeIfAbsent(cacheKey, () -> {
                        return jdbcTemplate.queryForList(sql.getExprSql(), sql.getArgs());
                    });
                    for (Map<String, Object> each : resultList) {
                        convertValueType(esMapping, mergeDataMap, each, esTemplate);
                    }
                    Object pkValue = mergeDataMap.get(objectField.getParentDocumentId());
                    if (pkValue != null) {
                        //更新ES文档 (执行完会统一提交, 这里不用commit)
                        esTemplate.update(esMapping, pkValue, Collections.singletonMap(
                                objectField.getFieldName(), resultList), bulkRequestList);
                    }
                    break;
                }
                case OBJECT_SQL: {
                    String cacheKey = "OBJECT_SQL_" + sql.getExprSql() + "_" + Arrays.toString(sql.getArgs());
                    Map<String, Object> resultMap = cacheMap.cacheComputeIfAbsent(cacheKey, () -> {
                        return jdbcTemplate.queryForMap(sql.getExprSql(), sql.getArgs());
                    });
                    convertValueType(esMapping, mergeDataMap, resultMap, esTemplate);
                    Object pkValue = mergeDataMap.get(objectField.getParentDocumentId());
                    if (pkValue != null) {
                        //更新ES文档 (执行完会统一提交, 这里不用commit)
                        esTemplate.update(esMapping, pkValue, Collections.singletonMap(
                                objectField.getFieldName(), resultMap), bulkRequestList);
                    }
                    break;
                }
                default: {
                }
            }
        }
    }

    public List<SQL> convertToSql(Dml dml) {
        List<SchemaItem> schemaItemList = schemaItemMap.get(dml.getTable());
        if (schemaItemList == null) {
            return Collections.emptyList();
        }
        if (CollectionUtils.isEmpty(dml.getData()) && CollectionUtils.isEmpty(dml.getOld())) {
            return Collections.emptyList();
        }
        List<SQL> sqlList = new ArrayList<>();
        forEachSqlField(schemaItemList, dml, (dmlIndex, schemaItem) -> {
            Map<String, Object> mergeDataMap = MERGE_DATA_MAP_THREAD_LOCAL.get();
            try {
                ESSyncConfig.ObjectField objectField = schemaItem.getObjectField();
                ESSyncConfig.ESMapping esMapping = objectField.getEsMapping();
                if (!CollectionUtils.isEmpty(dml.getOld()) && dmlIndex < dml.getOld().size()) {
                    mergeDataMap.putAll(dml.getOld().get(dmlIndex));
                }
                if (!CollectionUtils.isEmpty(dml.getData()) && dmlIndex < dml.getData().size()) {
                    mergeDataMap.putAll(dml.getData().get(dmlIndex));
                }
                if (mergeDataMap == null || mergeDataMap.isEmpty()) {
                    return false;
                }
                boolean isParentChange = dml.getTable().equals(esMapping.getSchemaItem().getMainTable().getTableName());
                String fullSql = objectField.getFullSql(isParentChange);
                SQL sql = convertToSql(fullSql, new HashMap<>(mergeDataMap), schemaItem);
                sqlList.add(sql);
                return true;
            } finally {
                if (mergeDataMap != null) {
                    mergeDataMap.clear();
                }
            }
        });
        return sqlList;
    }

    /**
     * sql语句
     */
    private static class SQL {
        private final String exprSql;
        private final Object[] args;
        private final Map<String, Object> argsMap;
        private final SchemaItem schemaItem;

        SQL(String exprSql, Object[] args, Map<String, Object> argsMap,
            SchemaItem schemaItem) {
            this.exprSql = exprSql;
            this.args = args;
            this.argsMap = argsMap;
            this.schemaItem = schemaItem;
        }

        String getExprSql() {
            return exprSql;
        }

        Object[] getArgs() {
            return args;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SQL)) return false;
            SQL sql = (SQL) o;
            return Objects.equals(exprSql, sql.exprSql) && Objects.deepEquals(args, sql.args) && Objects.equals(schemaItem, sql.schemaItem);
        }

        @Override
        public int hashCode() {
            return Objects.hash(exprSql, Arrays.hashCode(args), schemaItem);
        }

        @Override
        public String toString() {
            return "SQL{" +
                    "exprSql='" + exprSql + '\'' +
                    ", args=" + Arrays.toString(args) +
                    '}';
        }
    }

    /**
     * 占位符
     */
    private static class Placeholder {
        private String source;
        private int beginIndex;
        private int endIndex;

        Placeholder(String source, int beginIndex, int endIndex) {
            this.source = source;
            this.beginIndex = beginIndex;
            this.endIndex = endIndex;
        }

        int getBeginIndex() {
            return beginIndex;
        }

        int getEndIndex() {
            return endIndex;
        }

        public String getKey() {
            return source.substring(beginIndex, endIndex);
        }

        @Override
        public String toString() {
            return getKey();
        }
    }

}
