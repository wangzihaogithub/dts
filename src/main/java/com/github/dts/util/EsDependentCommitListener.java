package com.github.dts.util;

import com.github.dts.impl.elasticsearch7x.BasicFieldWriter;

import java.sql.Timestamp;
import java.util.List;

public interface EsDependentCommitListener {
    void done(CommitEvent event);

    static class CommitEvent {
        public final List<SqlDependent> mainTableSqlDependentList;
        public final List<SqlDependent> mainTableJoinSqlDependentList;
        public final List<SqlDependent> slaveTableSqlDependentList;
        public final Timestamp startTimestamp;
        public final Timestamp sqlTimestamp;
        public final BasicFieldWriter.WriteResult writeResult;

        public CommitEvent(BasicFieldWriter.WriteResult writeResult, List<SqlDependent> mainTableSqlDependentList, List<SqlDependent> mainTableJoinSqlDependentList, List<SqlDependent> slaveTableSqlDependentList, Timestamp startTimestamp, Timestamp sqlTimestamp) {
            this.writeResult = writeResult;
            this.mainTableSqlDependentList = mainTableSqlDependentList;
            this.mainTableJoinSqlDependentList = mainTableJoinSqlDependentList;
            this.slaveTableSqlDependentList = slaveTableSqlDependentList;
            this.startTimestamp = startTimestamp;
            this.sqlTimestamp = sqlTimestamp;
        }
    }
}