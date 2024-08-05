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
        switch (sqlList.size()) {
            case 0: {
                return Collections.emptyList();
            }
            case 1: {
                T first = sqlList.iterator().next();
                return Collections.singletonList(newNoMerge(new ArrayList<>(sqlList), first));
            }
            default: {
                Set<T> sqlSet = sqlList instanceof Set ? (Set<T>) sqlList : new LinkedHashSet<>(sqlList);
                Map<String, List<T>> groupByExprSqlMap = sqlSet.stream()
                        .collect(Collectors.groupingBy(e -> e.getExprSql() + "_" + e.getDataSourceKey() + "_" + e.getNeedGroupBy(), LinkedHashMap::new, Collectors.toList()));

                List<MergeJdbcTemplateSQL<T>> result = new ArrayList<>();
                for (List<T> valueList : groupByExprSqlMap.values()) {
                    T first = valueList.iterator().next();
                    if (valueList.size() == 1) {
                        result.add(newNoMerge(valueList, first));
                    } else {
                        Map<String, T> distinct = new LinkedHashMap<>();
                        for (T t : valueList) {
                            distinct.put(argsToString(t.getArgs()), t);
                        }
                        List<List<T>> partitionList = Lists.partition(new ArrayList<>(distinct.values()), maxIdInCount);
                        for (List<T> partition : partitionList) {
                            List<Object[]> argsList = partition.stream().map(SQL::getArgs).collect(Collectors.toList());
                            SqlParser.ChangeSQL changeSQL = SqlParser.changeMergeSelect(first.getExprSql(), argsList, first.getNeedGroupBy());
                            if (changeSQL != null) {
                                result.add(new MergeJdbcTemplateSQL<>(
                                        changeSQL.getSql(), changeSQL.getArgs(),
                                        first.getDataSourceKey(),
                                        changeSQL.getUniqueColumnNames(), changeSQL.getAddColumnNameList(),
                                        partition,
                                        first.getNeedGroupBy()));
                            } else {
                                for (T value : partition) {
                                    result.add(newNoMerge(partition, value));
                                }
                            }
                        }
                    }
                }
                return result;
            }
        }
    }

    private static <T extends JdbcTemplateSQL> MergeJdbcTemplateSQL<T> newNoMerge(List<T> mergeList, T value) {
        return new MergeJdbcTemplateSQL<>(
                value.getExprSql(), value.getArgs(),
                value.getDataSourceKey(),
                null, null,
                mergeList,
                value.getNeedGroupBy());
    }

    private static String argsToString(Object[] args) {
        StringJoiner joiner = new StringJoiner("_");
        for (Object arg : args) {
            joiner.add(String.valueOf(arg));
        }
        return joiner.toString();
    }

    public static <T extends JdbcTemplateSQL> Map<T, List<Map<String, Object>>> toMap(List<MergeJdbcTemplateSQL<T>> mergeList) {
        return toMap(mergeList, null);
    }

    public static <T extends JdbcTemplateSQL> Map<T, List<Map<String, Object>>> toMap(List<MergeJdbcTemplateSQL<T>> mergeList, CacheMap cacheMap) {
        Map<T, List<Map<String, Object>>> result = new HashMap<>();
        for (MergeJdbcTemplateSQL<T> templateSQL : mergeList) {
            Map<T, List<Map<String, Object>>> map = templateSQL.dispatch(templateSQL.executeQueryList(cacheMap));
            for (Map.Entry<T, List<Map<String, Object>>> entry : map.entrySet()) {
                T key = entry.getKey();
                result.computeIfAbsent(key, k -> new ArrayList<>())
                        .addAll(entry.getValue());
            }
        }
        return result;
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

    public void executeQueryStream(int chunkSize, Consumer<Chunk<T>> each) {
        Chunk<T> chunk = new Chunk<>();
        boolean merge = isMerge();

        Set<T> visited;
        IdentityHashMap<Object[], String> argsToStringCache;
        if (merge) {
            visited = Collections.newSetFromMap(new IdentityHashMap<>());
            argsToStringCache = new IdentityHashMap<>();
        } else {
            visited = null;
            argsToStringCache = null;
        }

        List<Map<String, Object>> list;
        int pageNo = 1;
        do {
            list = executeQueryList(null, pageNo, chunkSize);
            if (merge) {
                Map<String, List<Map<String, Object>>> getterMap = list.stream()
                        .collect(Collectors.groupingBy(e -> argsToString(getUniqueColumnValues(e))));
                for (T dependentSQL : mergeList) {
                    String uniqueColumnKey = argsToStringCache.computeIfAbsent(dependentSQL.getArgs(), MergeJdbcTemplateSQL::argsToString);
                    List<Map<String, Object>> rowList = getterMap.get(uniqueColumnKey);
                    if (rowList == null) {
                        continue;
                    }
                    removeAddColumnValueList(rowList);

                    visited.add(dependentSQL);
                    chunk.reset(dependentSQL, pageNo, chunkSize, rowList);
                    each.accept(chunk);
                }
            } else {
                for (T dependentSQL : mergeList) {
                    chunk.reset(dependentSQL, pageNo, chunkSize, list);
                    each.accept(chunk);
                }
            }
            pageNo++;
        } while (list.size() == chunkSize);

        if (visited != null) {
            for (T dependentSQL : mergeList) {
                if (!visited.contains(dependentSQL)) {
                    chunk.reset(dependentSQL, 1, chunkSize, Collections.emptyList());
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
        public T sql;
        public int pageNo;
        public int pageSize;
        public List<Map<String, Object>> rowList;

        public void reset(T sql, int pageNo, int pageSize, List<Map<String, Object>> rowList) {
            this.sql = sql;
            this.pageNo = pageNo;
            this.pageSize = pageSize;
            this.rowList = rowList;
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
