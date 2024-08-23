package com.github.dts.impl.elasticsearch7x.nested;

import com.github.dts.util.CacheMap;
import com.github.dts.util.SchemaItem;
import com.github.dts.util.SqlParser;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.RecoverableDataAccessException;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MergeJdbcTemplateSQL<T extends JdbcTemplateSQL> extends JdbcTemplateSQL {
    private static final Logger log = LoggerFactory.getLogger(MergeJdbcTemplateSQL.class);
    private final List<T> mergeList;
    private final String[] uniqueColumnNames;
    private final List<String> addColumnNameList;

    private MergeJdbcTemplateSQL(String exprSql, Object[] args,
                                 String dataSourceKey,
                                 String[] uniqueColumnNames,
                                 List<String> addColumnNameList,
                                 List<T> mergeList,
                                 Collection<SchemaItem.ColumnItem> needGroupBy) {
        super(exprSql, args, new LinkedHashMap<>(), dataSourceKey, needGroupBy);
        this.uniqueColumnNames = uniqueColumnNames;
        this.addColumnNameList = addColumnNameList;
        this.mergeList = mergeList;
    }

    public static <T extends JdbcTemplateSQL> void executeQueryList(
            List<MergeJdbcTemplateSQL<T>> sqlList,
            CacheMap cacheMap,
            BiConsumer<T, List<Map<String, Object>>> each) {
        for (MergeJdbcTemplateSQL<T> sql : sqlList) {
            try {
                List<Map<String, Object>> rowList = sql.executeQueryList(sql.isMerge() ? null : cacheMap);
                sql.dispatch(rowList, each);
            } catch (RecoverableDataAccessException e) {
                log.info("retry executeMergeUpdateES. size = {}, cause {}", sqlList.size(), e.toString());
                // 数据过大自动重试
                // fix：The last packet successfully received from the server was 10,233 milliseconds ago. The last packet sent successfully to the server was 11,726 milliseconds ago.
                for (T dependentSQL : sql.mergeList) {
                    each.accept(dependentSQL, dependentSQL.executeQueryListRetry(cacheMap, 1000));
                }
            }
        }
    }

    public static <T extends JdbcTemplateSQL> List<MergeJdbcTemplateSQL<T>> merge(Collection<T> sqlList, int maxIdInCount) {
        return merge(sqlList, maxIdInCount, true);
    }

    public static <T extends JdbcTemplateSQL> List<MergeJdbcTemplateSQL<T>> merge(Collection<T> sqlList, int maxIdInCount, boolean groupBy) {
        // !!这里保证入参的sqlList和返回的数量保持一样
        switch (sqlList.size()) {
            case 0: {
                return Collections.emptyList();
            }
            case 1: {
                T first = sqlList.iterator().next();
                return Collections.singletonList(newNoMerge(new ArrayList<>(sqlList), first, groupBy));
            }
            default: {
                Map<String, List<T>> groupByExprSqlMap = sqlList.stream()
                        .collect(Collectors.groupingBy(e -> e.getExprSql() + "_" + e.getDataSourceKey() + "_" + e.getNeedGroupBy(), LinkedHashMap::new, Collectors.toList()));

                List<MergeJdbcTemplateSQL<T>> result = new ArrayList<>();
                for (List<T> valueList : groupByExprSqlMap.values()) {
                    T first = valueList.iterator().next();
                    if (valueList.size() == 1) {
                        result.add(newNoMerge(valueList, first, groupBy));
                    } else {
                        List<List<T>> partitionList = Lists.partition(valueList, maxIdInCount);
                        for (List<T> partition : partitionList) {
                            Collection<SchemaItem.ColumnItem> needGroupBy = groupBy ? first.getNeedGroupBy() : null;
                            Map<String, Object[]> distinctArgList = new LinkedHashMap<>();
                            for (T e : partition) {
                                distinctArgList.put(argsToString(e.getArgs()), e.getArgs());
                            }
                            SqlParser.ChangeSQL changeSQL = SqlParser.changeMergeSelect(first.getExprSql(), distinctArgList.values(), needGroupBy);
                            if (changeSQL != null) {
                                result.add(new MergeJdbcTemplateSQL<>(
                                        changeSQL.getSql(), changeSQL.getArgs(),
                                        first.getDataSourceKey(),
                                        changeSQL.getUniqueColumnNames(), changeSQL.getAddColumnNameList(),
                                        partition,
                                        needGroupBy));
                            } else {
                                for (T value : partition) {
                                    result.add(newNoMerge(Collections.singletonList(value), value, groupBy));
                                }
                            }
                        }
                    }
                }
                return result;
            }
        }
    }

    private static <T extends JdbcTemplateSQL> MergeJdbcTemplateSQL<T> newNoMerge(List<T> mergeList, T value, boolean groupBy) {
        Collection<SchemaItem.ColumnItem> needGroupBy = value.getNeedGroupBy();
        String exprSql = groupBy && needGroupBy != null && !needGroupBy.isEmpty() ?
                SqlParser.setGroupBy(value.getExprSql(), value.getNeedGroupBy()) : value.getExprSql();
        return new MergeJdbcTemplateSQL<>(
                exprSql, value.getArgs(),
                value.getDataSourceKey(),
                null, null,
                mergeList,
                null);
    }

    public static String argsToString(Object[] args) {
        StringJoiner joiner = new StringJoiner("_");
        for (Object arg : args) {
            joiner.add(String.valueOf(arg));
        }
        return joiner.toString();
    }

    public static <T extends JdbcTemplateSQL, K> Map<K, List<Map<String, Object>>> toMap(List<MergeJdbcTemplateSQL<T>> mergeList, Function<T, K> key) {
        return toMap(mergeList, key, null);
    }

    public static <T extends JdbcTemplateSQL, K> Map<K, List<Map<String, Object>>> toMap(List<MergeJdbcTemplateSQL<T>> mergeList, Function<T, K> key, CacheMap cacheMap) {
        Map<K, List<Map<String, Object>>> result = new HashMap<>();
        for (MergeJdbcTemplateSQL<T> templateSQL : mergeList) {
            Map<T, List<Map<String, Object>>> map = templateSQL.dispatch(templateSQL.executeQueryList(cacheMap));
            for (Map.Entry<T, List<Map<String, Object>>> entry : map.entrySet()) {
                result.computeIfAbsent(key.apply(entry.getKey()), k -> new ArrayList<>())
                        .addAll(entry.getValue());
            }
        }
        return result;
    }

    private static String lastKey(Map<String, List<Map<String, Object>>> getterMap) {
        String last = null;
        for (String s : getterMap.keySet()) {
            last = s;
        }
        return last;
    }

    public boolean isMerge() {
        return uniqueColumnNames != null;
    }

    public Map<T, List<Map<String, Object>>> toMap() {
        return dispatch(executeQueryList(null));
    }

    public Map<T, List<Map<String, Object>>> toMap(CacheMap cacheMap) {
        return dispatch(executeQueryList(cacheMap));
    }

    public <K> void executeQueryStream(int chunkSize, Function<T, K> key, Consumer<Chunk<K>> each) {
        Chunk<K> chunk = new Chunk<>();
        boolean merge = isMerge();

        Set<K> visited;
        IdentityHashMap<Object[], String> argsToStringCache = new IdentityHashMap<>();
        if (merge) {
            visited = Collections.newSetFromMap(new IdentityHashMap<>());
        } else {
            visited = null;
        }
        Map<K, Set<String>> keyUniqueColumnKeyMap = new HashMap<>(mergeList.size());
        Set<K> keySet = new LinkedHashSet<>(mergeList.size());
        Map<T, K> keyCache = new IdentityHashMap<>(mergeList.size());
        for (T t : mergeList) {
            K k = key.apply(t);
            keyCache.put(t, k);
            keySet.add(k);
            keyUniqueColumnKeyMap.computeIfAbsent(k, (o) -> new LinkedHashSet<>())
                    .add(argsToStringCache.computeIfAbsent(t.getArgs(), MergeJdbcTemplateSQL::argsToString));
        }
        List<Map<String, Object>> lastRow = Collections.emptyList();
        String lastKey = null;
        List<Map<String, Object>> list;
        int pageNo = 1;
        do {
            list = executeQueryList(null, pageNo, chunkSize);
            if (merge) {
                Map<String, List<Map<String, Object>>> getterMap = list.stream()
                        .collect(Collectors.groupingBy(e -> argsToString(getUniqueColumnValues(e)), LinkedHashMap::new, Collectors.toList()));

                // 解决拆包黏包问题
                if (lastKey != null) {
                    List<Map<String, Object>> beforeLastRow = getterMap.get(lastKey);
                    if (beforeLastRow == null) {
                        getterMap.put(lastKey, lastRow);
                    } else {
                        beforeLastRow.addAll(lastRow);
                    }
                }
                if (list.size() == chunkSize) {
                    lastKey = lastKey(getterMap);
                    lastRow = getterMap.remove(lastKey);
                } else {
                    lastKey = null;
                    lastRow = Collections.emptyList();
                }
                // 解决拆包黏包问题

                for (K k : keySet) {
                    Set<String> keyUniqueColumnKeySet = keyUniqueColumnKeyMap.get(k);
                    for (String uniqueColumnKey : keyUniqueColumnKeySet) {
                        List<Map<String, Object>> rowList = getterMap.get(uniqueColumnKey);
                        if (rowList == null) {
                            continue;
                        }
                        removeAddColumnValueList(rowList);

                        visited.add(k);
                        chunk.reset(k, pageNo, chunkSize, rowList, uniqueColumnKey);
                        each.accept(chunk);
                    }
                }
            } else {
                for (K k : keySet) {
                    Set<String> keyUniqueColumnKeySet = keyUniqueColumnKeyMap.get(k);
                    for (String keyUniqueColumnKey : keyUniqueColumnKeySet) {
                        chunk.reset(k, pageNo, chunkSize, list, keyUniqueColumnKey);
                        each.accept(chunk);
                    }
                }
            }
            pageNo++;
        } while (list.size() == chunkSize);

        if (visited != null) {
            for (T dependentSQL : mergeList) {
                K k = keyCache.get(dependentSQL);
                if (!visited.contains(k)) {
                    String keyUniqueColumnKey = argsToStringCache.computeIfAbsent(dependentSQL.getArgs(), MergeJdbcTemplateSQL::argsToString);
                    chunk.reset(k, 1, chunkSize, Collections.emptyList(), keyUniqueColumnKey);
                    each.accept(chunk);
                }
            }
        }
    }

    public Map<T, List<Map<String, Object>>> dispatch(List<Map<String, Object>> list) {
        Map<T, List<Map<String, Object>>> result = new LinkedHashMap<>();
        dispatch(list, result::put);
        return result;
    }

    public void dispatch(List<Map<String, Object>> list, BiConsumer<T, List<Map<String, Object>>> each) {
        if (isMerge()) {
            Map<String, List<Map<String, Object>>> getterMap = list.stream()
                    .collect(Collectors.groupingBy(e -> argsToString(getUniqueColumnValues(e))));
            for (T dependentSQL : mergeList) {
                String uniqueColumnKey = argsToString(dependentSQL.getArgs());
                List<Map<String, Object>> rowList = getterMap.getOrDefault(uniqueColumnKey, Collections.emptyList());
                removeAddColumnValueList(rowList);
                each.accept(dependentSQL, rowList);
            }
        } else {
            for (T dependentSQL : mergeList) {
                each.accept(dependentSQL, list);
            }
        }
    }

    private Object[] getUniqueColumnValues(Map<String, Object> row) {
        Object[] args = new Object[uniqueColumnNames.length];
        for (int i = 0; i < uniqueColumnNames.length; i++) {
            args[i] = row.get(uniqueColumnNames[i]);
        }
        return args;
    }

    private void removeAddColumnValueList(List<Map<String, Object>> rowList) {
        for (Map<String, Object> row : rowList) {
            for (String addColumnName : addColumnNameList) {
                row.remove(addColumnName);
            }
        }
    }

    public static class Chunk<T> {
        public T source;
        public int pageNo;
        public int pageSize;
        public List<Map<String, Object>> rowList;
        public String uniqueColumnKey;

        public void reset(T sql, int pageNo, int pageSize, List<Map<String, Object>> rowList, String uniqueColumnKey) {
            this.source = sql;
            this.pageNo = pageNo;
            this.pageSize = pageSize;
            this.rowList = rowList;
            this.uniqueColumnKey = uniqueColumnKey;
        }

        public List<Object> rowListFirst() {
            List<Object> rowListFirst = new ArrayList<>(rowList.size());
            for (Map<String, Object> row : rowList) {
                if (row.isEmpty()) {
                    continue;
                }
                Object pkValue = row.values().iterator().next();
                rowListFirst.add(pkValue);
            }
            return rowListFirst;
        }

        public List<Object> rowListFirst(Set<String> visited, String dmlUniqueColumnKey) {
            List<Object> rowListFirst = new ArrayList<>(rowList.size());
            for (Map<String, Object> row : rowList) {
                if (row.isEmpty()) {
                    continue;
                }
                Object pkValue = row.values().iterator().next();
                if (visited.add(pkValue + "_" + dmlUniqueColumnKey)) {
                    rowListFirst.add(pkValue);
                }
            }
            return rowListFirst;
        }

        public boolean hasNext() {
            return rowList.size() == pageSize;
        }

        @Override
        public String toString() {
            return "Chunk{" +
                    "pageNo=" + pageNo +
                    ", pageSize=" + pageSize +
                    ", hasNext=" + hasNext() +
                    '}';
        }
    }
}
