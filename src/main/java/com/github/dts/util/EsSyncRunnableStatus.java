package com.github.dts.util;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class EsSyncRunnableStatus {

    private List<Row> rows = new ArrayList<>();

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    public static class Row {
        private Timestamp binlogTimestamp;
        private long total;
        private long curr;
        private String info;

        public Timestamp getBinlogTimestamp() {
            return binlogTimestamp;
        }

        public void setBinlogTimestamp(Timestamp binlogTimestamp) {
            this.binlogTimestamp = binlogTimestamp;
        }

        public long getTotal() {
            return total;
        }

        public void setTotal(long total) {
            this.total = total;
        }

        public long getCurr() {
            return curr;
        }

        public void setCurr(long curr) {
            this.curr = curr;
        }

        public String getInfo() {
            return info;
        }

        public void setInfo(String info) {
            this.info = info;
        }
    }
}
