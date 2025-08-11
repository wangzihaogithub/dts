package com.github.dts.util;

import java.util.*;
import java.util.stream.Collectors;

public class SqlDependentGroup {
    /**
     * 是否受到依赖影响
     */
    private final boolean onlyEffect;
    /**
     * 处理逻辑分类
     *
     * @see com.github.dts.impl.elasticsearch.NestedFieldWriter
     */
    private final List<SqlDependent> mainTableSqlDependentList;
    /**
     * 处理逻辑分类
     *
     * @see com.github.dts.impl.elasticsearch.NestedMainJoinTableRunnable
     */
    private final List<SqlDependent> mainTableJoinSqlDependentList;
    /**
     * 处理逻辑分类
     *
     * @see com.github.dts.impl.elasticsearch.NestedSlaveTableRunnable
     */
    private final List<SqlDependent> slaveTableSqlDependentList;

    public SqlDependentGroup() {
        this.onlyEffect = false;
        this.mainTableSqlDependentList = new ArrayList<>();
        this.mainTableJoinSqlDependentList = new ArrayList<>();
        this.slaveTableSqlDependentList = new ArrayList<>();
    }

    public SqlDependentGroup(List<SqlDependentGroup> list, boolean onlyEffect) {
        this.onlyEffect = onlyEffect;
        this.mainTableSqlDependentList = new ArrayList<>(list.stream().mapToInt(e -> e.mainTableSqlDependentList.size()).sum());
        this.mainTableJoinSqlDependentList = new ArrayList<>(list.stream().mapToInt(e -> e.mainTableJoinSqlDependentList.size()).sum());
        this.slaveTableSqlDependentList = new ArrayList<>(list.stream().mapToInt(e -> e.slaveTableSqlDependentList.size()).sum());
        for (SqlDependentGroup group : list) {
            mainTableSqlDependentList.addAll(group.mainTableSqlDependentList);
            mainTableJoinSqlDependentList.addAll(group.mainTableJoinSqlDependentList);
            slaveTableSqlDependentList.addAll(group.slaveTableSqlDependentList);
        }
    }

    public Collection<String> getIndices() {
        Set<String> indices = new LinkedHashSet<>();
        for (SqlDependent sqlDependent : mainTableSqlDependentList) {
            indices.add(sqlDependent.getSchemaItem().getEsMapping().get_index());
        }
        for (SqlDependent sqlDependent : mainTableJoinSqlDependentList) {
            indices.add(sqlDependent.getSchemaItem().getEsMapping().get_index());
        }
        for (SqlDependent sqlDependent : slaveTableSqlDependentList) {
            indices.add(sqlDependent.getSchemaItem().getEsMapping().get_index());
        }
        return indices;
    }

    public List<SqlDependent> getMainTableDependentList() {
        return Collections.unmodifiableList(mainTableSqlDependentList);
    }

    public List<SqlDependent> getSlaveTableDependentList() {
        return Collections.unmodifiableList(slaveTableSqlDependentList);
    }

    public List<SqlDependent> getMainTableJoinDependentList() {
        return Collections.unmodifiableList(mainTableJoinSqlDependentList);
    }

    public List<SqlDependent> selectMainTableSqlDependentList(Collection<String> onlyFieldName) {
        List<SqlDependent> result;
        if (onlyFieldName == null) {
            result = mainTableSqlDependentList;
        } else {
            if (onlyFieldName.isEmpty()) {
                result = Collections.emptyList();
            } else {
                result = mainTableSqlDependentList.stream()
                        .filter(e -> e.containsObjectField(onlyFieldName))
                        .collect(Collectors.toList());
            }
        }
        return Collections.unmodifiableList(filterEffect(result));
    }

    public List<SqlDependent> selectSlaveTableDependentList() {
        return Collections.unmodifiableList(filterEffect(slaveTableSqlDependentList));
    }

    public List<SqlDependent> selectMainTableJoinDependentList() {
        return Collections.unmodifiableList(filterEffect(mainTableJoinSqlDependentList));
    }

    private List<SqlDependent> filterEffect(List<SqlDependent> list) {
        return onlyEffect ? list.stream().filter(SqlDependent::isEffect).collect(Collectors.toList()) : list;
    }

    public void addMain(SqlDependent sqlDependent) {
        mainTableSqlDependentList.add(sqlDependent);
    }

    public void addMainJoin(SqlDependent sqlDependent) {
        mainTableJoinSqlDependentList.add(sqlDependent);
    }

    public void addSlave(SqlDependent sqlDependent) {
        slaveTableSqlDependentList.add(sqlDependent);
    }

    public void add(SqlDependentGroup sqlDependentGroup) {
        mainTableSqlDependentList.addAll(sqlDependentGroup.mainTableSqlDependentList);
        mainTableJoinSqlDependentList.addAll(sqlDependentGroup.mainTableJoinSqlDependentList);
        slaveTableSqlDependentList.addAll(sqlDependentGroup.slaveTableSqlDependentList);
    }

    @Override
    public String toString() {
        return "DependentGroup{" +
                "mainTableSize=" + mainTableSqlDependentList.size() +
                ", mainTableJoinSize=" + mainTableJoinSqlDependentList.size() +
                ", slaveTableSize=" + slaveTableSqlDependentList.size() +
                '}';
    }
}
   