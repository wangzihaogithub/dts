package com.github.dts.util;

import java.util.*;
import java.util.stream.Collectors;

public class DependentGroup {
    private final List<Dependent> mainTableDependentList = new ArrayList<>();
    private final List<Dependent> mainTableJoinDependentList = new ArrayList<>();
    private final List<Dependent> slaveTableDependentList = new ArrayList<>();

    public Collection<String> getIndices() {
        Set<String> indices = new LinkedHashSet<>();
        for (Dependent dependent : mainTableDependentList) {
            indices.add(dependent.getSchemaItem().getEsMapping().get_index());
        }
        for (Dependent dependent : mainTableJoinDependentList) {
            indices.add(dependent.getSchemaItem().getEsMapping().get_index());
        }
        for (Dependent dependent : slaveTableDependentList) {
            indices.add(dependent.getSchemaItem().getEsMapping().get_index());
        }
        return indices;
    }

    public List<Dependent> selectMainTableDependentList(Collection<String> onlyFieldName) {
        if (onlyFieldName == null) {
            return mainTableDependentList;
        } else {
            if (onlyFieldName.isEmpty()) {
                return Collections.emptyList();
            } else {
                return mainTableDependentList.stream()
                        .filter(e -> e.containsObjectField(onlyFieldName))
                        .collect(Collectors.toList());
            }
        }
    }

    public List<Dependent> getMainTableDependentList() {
        return mainTableDependentList;
    }

    public List<Dependent> getSlaveTableDependentList() {
        return slaveTableDependentList;
    }

    public List<Dependent> getMainTableJoinDependentList() {
        return mainTableJoinDependentList;
    }

    public void addMain(Dependent dependent) {
        mainTableDependentList.add(dependent);
    }

    public void addMainJoin(Dependent dependent) {
        mainTableJoinDependentList.add(dependent);
    }

    public void addSlave(Dependent dependent) {
        slaveTableDependentList.add(dependent);
    }

    public void add(DependentGroup dependentGroup) {
        mainTableDependentList.addAll(dependentGroup.mainTableDependentList);
        mainTableJoinDependentList.addAll(dependentGroup.mainTableJoinDependentList);
        slaveTableDependentList.addAll(dependentGroup.slaveTableDependentList);
    }

    public boolean isEmpty() {
        return mainTableDependentList.isEmpty() && mainTableJoinDependentList.isEmpty() && slaveTableDependentList.isEmpty();
    }

    @Override
    public String toString() {
        return "DependentGroup{" +
                "mainTableSize=" + mainTableDependentList.size() +
                ", mainTableJoinSize=" + mainTableJoinDependentList.size() +
                ", slaveTableSize=" + slaveTableDependentList.size() +
                '}';
    }
}
   