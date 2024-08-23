package com.github.dts.util;

import java.util.*;
import java.util.stream.Collectors;

public class DependentGroup {
    private final boolean onlyEffect;
    private final List<Dependent> mainTableDependentList;
    private final List<Dependent> mainTableJoinDependentList;
    private final List<Dependent> slaveTableDependentList;

    public DependentGroup() {
        this.onlyEffect = false;
        this.mainTableDependentList = new ArrayList<>();
        this.mainTableJoinDependentList = new ArrayList<>();
        this.slaveTableDependentList = new ArrayList<>();
    }

    public DependentGroup(List<DependentGroup> list, boolean onlyEffect) {
        this.onlyEffect = onlyEffect;
        this.mainTableDependentList = new ArrayList<>(list.stream().mapToInt(e -> e.mainTableDependentList.size()).sum());
        this.mainTableJoinDependentList = new ArrayList<>(list.stream().mapToInt(e -> e.mainTableJoinDependentList.size()).sum());
        this.slaveTableDependentList = new ArrayList<>(list.stream().mapToInt(e -> e.slaveTableDependentList.size()).sum());
        for (DependentGroup group : list) {
            mainTableDependentList.addAll(group.mainTableDependentList);
            mainTableJoinDependentList.addAll(group.mainTableJoinDependentList);
            slaveTableDependentList.addAll(group.slaveTableDependentList);
        }
    }

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

    public List<Dependent> getMainTableDependentList() {
        return Collections.unmodifiableList(mainTableDependentList);
    }

    public List<Dependent> getSlaveTableDependentList() {
        return Collections.unmodifiableList(slaveTableDependentList);
    }

    public List<Dependent> getMainTableJoinDependentList() {
        return Collections.unmodifiableList(mainTableJoinDependentList);
    }

    public List<Dependent> selectMainTableDependentList(Collection<String> onlyFieldName) {
        List<Dependent> result;
        if (onlyFieldName == null) {
            result = mainTableDependentList;
        } else {
            if (onlyFieldName.isEmpty()) {
                result = Collections.emptyList();
            } else {
                result = mainTableDependentList.stream()
                        .filter(e -> e.containsObjectField(onlyFieldName))
                        .collect(Collectors.toList());
            }
        }
        return Collections.unmodifiableList(filterEffect(result));
    }

    public List<Dependent> selectSlaveTableDependentList() {
        return Collections.unmodifiableList(filterEffect(slaveTableDependentList));
    }

    public List<Dependent> selectMainTableJoinDependentList() {
        return Collections.unmodifiableList(filterEffect(mainTableJoinDependentList));
    }

    private List<Dependent> filterEffect(List<Dependent> list) {
        return onlyEffect ? list.stream().filter(Dependent::isEffect).collect(Collectors.toList()) : list;
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

    @Override
    public String toString() {
        return "DependentGroup{" +
                "mainTableSize=" + mainTableDependentList.size() +
                ", mainTableJoinSize=" + mainTableJoinDependentList.size() +
                ", slaveTableSize=" + slaveTableDependentList.size() +
                '}';
    }
}
   