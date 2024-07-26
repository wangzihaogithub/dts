package com.github.dts.util;

import java.util.ArrayList;
import java.util.List;

public class DependentGroup {
    private final List<Dependent> mainTableDependentList = new ArrayList<>();
    private final List<Dependent> mainTableJoinDependentList = new ArrayList<>();
    private final List<Dependent> slaveTableDependentList = new ArrayList<>();

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
}
   