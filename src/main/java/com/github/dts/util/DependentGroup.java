package com.github.dts.util;

import java.util.ArrayList;
import java.util.List;

public class DependentGroup {
    private final List<Dependent> mainTableDependentList = new ArrayList<>();
    private final List<Dependent> slaveTableDependentList = new ArrayList<>();

    public List<Dependent> getMainTableDependentList() {
        return mainTableDependentList;
    }

    public List<Dependent> getSlaveTableDependentList() {
        return slaveTableDependentList;
    }

    public void addMain(Dependent dependent) {
        mainTableDependentList.add(dependent);
    }

    public void addSlave(Dependent dependent) {
        slaveTableDependentList.add(dependent);
    }

    public void add(DependentGroup dependentGroup) {
        mainTableDependentList.addAll(dependentGroup.mainTableDependentList);
        slaveTableDependentList.addAll(dependentGroup.slaveTableDependentList);
    }
}
   