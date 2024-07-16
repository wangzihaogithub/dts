package com.github.dts.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * DML操作转换对象
 *
 * @author rewerma 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public class Dml implements Serializable {

    private static final long serialVersionUID = 2611556444074013268L;
    private int index = 0;
    private String packetId;

    private String[] destination;                            // 对应canal的实例或者MQ的topic
    private String groupId;                                // 对应mq的group id
    private String database;                               // 数据库或schema
    private String table;                                  // 表名
    private List<String> pkNames;
    private Boolean isDdl;
    private String type;                                   // 类型: INSERT UPDATE DELETE
    // binlog executeTime
    private Long es;                                     // 执行耗时
    // dml build timeStamp
    private Long ts;                                     // 同步时间
    private String sql;                                    // 执行的sql, dml sql为空
    private List<Map<String, Object>> data;                                   // 数据列表
    private List<Map<String, Object>> old;                                    // 旧数据列表, 用于update, size和data的size一一对应

    private long logfileOffset;
    private long eventLength;
    private String logfileName;

    private Map<String, String> mysqlType;

    public static List<Dml> convertInsert(List<Map<String, Object>> rowList,
                                          List<String> pkNames,
                                          String table,
                                          String catalog) {
        return convertInsert(rowList, pkNames, table, catalog, null);
    }

    public static List<Dml> convertInsert(List<Map<String, Object>> rowList,
                                          List<String> pkNames,
                                          String table,
                                          String catalog, String[] destination) {
        List<Dml> dmlList = new ArrayList<>();
        for (Map row : rowList) {
            Dml dml = new Dml();
            dml.setData(Arrays.asList(row));
            dml.setTable(table);
            dml.setEs(System.currentTimeMillis());
            dml.setTs(System.currentTimeMillis());
            dml.setType("INSERT");
            dml.setIsDdl(false);
            dml.setPkNames(pkNames);
            dml.setDatabase(catalog);
            dml.setDestination(destination);
            dmlList.add(dml);
        }
        return dmlList;
    }

    public Map<String, String> getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(Map<String, String> mysqlType) {
        this.mysqlType = mysqlType;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getPacketId() {
        return packetId;
    }

    public void setPacketId(String packetId) {
        this.packetId = packetId;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public Boolean getDdl() {
        return isDdl;
    }

    public void setDdl(Boolean ddl) {
        isDdl = ddl;
    }

    public long getLogfileOffset() {
        return logfileOffset;
    }

    public void setLogfileOffset(long logfileOffset) {
        this.logfileOffset = logfileOffset;
    }

    public long getEventLength() {
        return eventLength;
    }

    public void setEventLength(long eventLength) {
        this.eventLength = eventLength;
    }

    public String getLogfileName() {
        return logfileName;
    }

    public void setLogfileName(String logfileName) {
        this.logfileName = logfileName;
    }

    public String[] getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination == null ? null : destination.isEmpty() ? new String[0] : destination.split(",");
    }

    public void setDestination(String[] destination) {
        this.destination = destination;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

//    public List<String> getPkNames() {
//        return pkNames;
//    }
//
//    public void setPkNames(List<String> pkNames) {
//        this.pkNames = pkNames;
//    }

    public Boolean getIsDdl() {
        return isDdl;
    }

    public void setIsDdl(Boolean isDdl) {
        this.isDdl = isDdl;
    }

    public boolean isTypeDelete(){
        return "DELETE".equalsIgnoreCase(type);
    }

    public boolean isTypeUpdate(){
        return "UPDATE".equalsIgnoreCase(type);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public List<Map<String, Object>> getOld() {
        return old;
    }

    public void setOld(List<Map<String, Object>> old) {
        this.old = old;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public void clear() {
        database = null;
        table = null;
        type = null;
        ts = null;
        es = null;
        data = null;
        old = null;
        sql = null;
    }

    @Override
    public String toString() {
        return "Dml{" + "destination='" + Arrays.toString(destination) + '\'' + ", database='" + database + '\'' + ", table='" + table
                + '\'' + ", type='" + type + '\'' + ", es=" + es + ", ts=" + ts + ", sql='" + sql + '\'' + ", data="
                + data + ", old=" + old + '}';
    }
}
