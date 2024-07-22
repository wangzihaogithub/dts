package com.github.dts.impl.elasticsearch7x.nested;

import com.github.dts.util.CacheMap;
import com.github.dts.util.SqlParser;
import com.google.common.collect.Lists;

import java.util.*;
import java.util.stream.Collectors;

public class MergeJdbcTemplateSQL<T extends JdbcTemplateSQL> extends JdbcTemplateSQL {
    private final List<T> mergeList;
    private final String[] uniqueColumnNames;
    private final List<String> addColumnNameList;

    private MergeJdbcTemplateSQL(String exprSql, Object[] args,
                                 String dataSourceKey,
                                 String[] uniqueColumnNames,
                                 List<String> addColumnNameList,
                                 List<T> mergeList,
                                 boolean needGroupBy) {
        super(exprSql, args, new LinkedHashMap<>(), dataSourceKey, needGroupBy);
        this.uniqueColumnNames = uniqueColumnNames;
        this.addColumnNameList = addColumnNameList;
        this.mergeList = mergeList;
    }

    public static <T extends JdbcTemplateSQL> Map<T, List<Map<String, Object>>> executeQueryList(
            List<MergeJdbcTemplateSQL<T>> sqlList,
            CacheMap cacheMap) {
        Map<T, List<Map<String, Object>>> result = new LinkedHashMap<>();
        for (MergeJdbcTemplateSQL<T> sql : sqlList) {
            List<Map<String, Object>> rowList = sql.executeQueryList(sql.isMerge() ? null : cacheMap);
            result.putAll(sql.dispatch(rowList));
        }
        return result;
    }

    public static <T extends JdbcTemplateSQL> List<MergeJdbcTemplateSQL<T>> merge(Collection<T> sqlList, int maxIdInCount) {
        switch (sqlList.size()) {
            case 0: {
                return Collections.emptyList();
            }
            case 1: {
                T first = sqlList.iterator().next();
                return Collections.singletonList(
                        new MergeJdbcTemplateSQL<T>(
                                first.getExprSql(), first.getArgs(),
                                first.getDataSourceKey(),
                                null, null,
                                new ArrayList<>(sqlList), first.isNeedGroupBy())
                );
            }
            default: {
                Set<T> sqlSet = sqlList instanceof Set ? (Set<T>) sqlList : new LinkedHashSet<>(sqlList);
                Map<String, List<T>> groupByExprSqlMap = sqlSet.stream()
                        .collect(Collectors.groupingBy(e -> e.getExprSql() + "_" + e.getDataSourceKey() + "_" + e.isNeedGroupBy(), LinkedHashMap::new, Collectors.toList()));

                List<MergeJdbcTemplateSQL<T>> result = new ArrayList<>();
                for (List<T> valueList : groupByExprSqlMap.values()) {
                    T first = valueList.iterator().next();
                    List<List<T>> partition = Lists.partition(valueList, maxIdInCount);
                    for (List<T> list : partition) {
                        List<Object[]> argsList = list.stream().map(SQL::getArgs).collect(Collectors.toList());
                        List<SqlParser.ChangeSQL> changeSQLList = SqlParser.changeMergeSelect(first.getExprSql(), argsList, first.isNeedGroupBy());
                        if (changeSQLList != null) {
                            for (SqlParser.ChangeSQL changeSQL : changeSQLList) {
                                result.add(new MergeJdbcTemplateSQL<>(
                                        changeSQL.getSql(), changeSQL.getArgs(),
                                        first.getDataSourceKey(),
                                        changeSQL.getUniqueColumnNames(), changeSQL.getAddColumnNameList(),
                                        list,
                                        first.isNeedGroupBy()));
                            }
                        } else {
                            for (T value : list) {
                                result.add(new MergeJdbcTemplateSQL<>(
                                        value.getExprSql(), value.getArgs(),
                                        value.getDataSourceKey(),
                                        null, null,
                                        list,
                                        value.isNeedGroupBy()));
                            }
                        }
                    }

                }
                return result;
            }
        }
    }

    private static String argsToString(Object[] args) {
        StringJoiner joiner = new StringJoiner("_");
        for (Object arg : args) {
            joiner.add(String.valueOf(arg));
        }
        return joiner.toString();
    }

    public boolean isMerge() {
        return uniqueColumnNames != null;
    }

    public Map<T, List<Map<String, Object>>> dispatch(List<Map<String, Object>> list) {
        Map<T, List<Map<String, Object>>> result = new LinkedHashMap<>();
        if (isMerge()) {
            Map<String, List<Map<String, Object>>> getterMap = list.stream()
                    .collect(Collectors.groupingBy(e -> argsToString(getUniqueColumnValues(e))));
            for (T dependentSQL : mergeList) {
                String uniqueColumnKey = argsToString(dependentSQL.getArgs());
                List<Map<String, Object>> rowList = getterMap.getOrDefault(uniqueColumnKey, Collections.emptyList());
                removeAddColumnValueList(rowList);
                result.put(dependentSQL, rowList);
            }
        } else {
            for (T dependentSQL : mergeList) {
                result.put(dependentSQL, list);
            }
        }
        return result;
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
}
