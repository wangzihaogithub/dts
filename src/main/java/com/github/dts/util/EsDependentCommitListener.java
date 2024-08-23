package com.github.dts.util;

import com.github.dts.impl.elasticsearch7x.BasicFieldWriter;

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
        public final BasicFieldWriter.WriteResult writeResult;

        public CommitEvent(BasicFieldWriter.WriteResult writeResult, List<Dependent> mainTableDependentList, List<Dependent> mainTableJoinDependentList, List<Dependent> slaveTableDependentList, Timestamp startTimestamp, Timestamp sqlTimestamp) {
            this.writeResult = writeResult;
            this.mainTableDependentList = mainTableDependentList;
            this.mainTableJoinDependentList = mainTableJoinDependentList;
            this.slaveTableDependentList = slaveTableDependentList;
            this.startTimestamp = startTimestamp;
            this.sqlTimestamp = sqlTimestamp;
        }
    }
}