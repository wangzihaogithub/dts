package com.github.dts.util;

import java.sql.Timestamp;
import java.util.List;

public interface EsDependentCommitListener {
    void done(CommitEvent event);

    static class CommitEvent {
        public final List<Dependent> mainTableDependentList;
        public final List<Dependent> mainTableJoinDependentList;
        public final List<Dependent> slaveTableDependentList;

        public final Timestamp startTimestamp;
        public final Timestamp sqlTimestamp;

        public CommitEvent(List<Dependent> mainTableDependentList, List<Dependent> mainTableJoinDependentList, List<Dependent> slaveTableDependentList, Timestamp startTimestamp, Timestamp sqlTimestamp) {
            this.mainTableDependentList = mainTableDependentList;
            this.mainTableJoinDependentList = mainTableJoinDependentList;
            this.slaveTableDependentList = slaveTableDependentList;
            this.startTimestamp = startTimestamp;
            this.sqlTimestamp = sqlTimestamp;
        }
    }
}