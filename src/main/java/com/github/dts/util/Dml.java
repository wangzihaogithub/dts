package com.github.dts.util;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.io.Serializable;
import java.util.*;

/**
 * DML操作转换对象
 *
 * @author rewerma 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public class Dml implements Serializable {
    public static final String TYPE_TRANSACTIONBEGIN = CanalEntry.EntryType.TRANSACTIONBEGIN.toString();
    public static final String TYPE_TRANSACTIONEND = CanalEntry.EntryType.TRANSACTIONEND.toString();

    private static final long serialVersionUID = 2611556444074013268L;
    private int index = 0;
    private String packetId;

    private String[] destination;                            // 对应canal的实例或者MQ的topic
    private String groupId;                                // 对应mq的group id
    private String database;                               // 数据库或schema
    private String table;                                  // 表名
    private List<String> pkNames;
    private Boolean isDdl;
    private String type;                                   // 类型: INSERT ｜ UPDATE ｜ DELETE ｜ TRANSACTIONBEGIN | TRANSACTIONEND | CREATE ｜ ALTER ｜ ERASE ｜ QUERY ｜ TRUNCATE ｜ RENAME ｜ CINDEX ｜ DINDEX ｜ GTID ｜ XACOMMIT ｜ XAROLLBACK ｜ MHEARTBEAT
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

    /**
     * 过滤掉事物消息
     *
     * @param list list
     * @return 过滤掉事物消息
     */
    public static List<Dml> filterTransaction(List<Dml> list) {
        ArrayList<Dml> result= new ArrayList<>(list.size());
        for (Dml e : list) {
            if (!TYPE_TRANSACTIONBEGIN.equals(e.type) && !TYPE_TRANSACTIONEND.equals(e.type)) {
                result.add(e);
            }
        }
        return result;
    }

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
        long timeMillis = System.currentTimeMillis();

        List<Dml> dmlList = new ArrayList<>();
        for (Map row : rowList) {
            Dml dml = new Dml();
            dml.setData(Collections.singletonList(row));
            dml.setTable(table);
            dml.setEs(timeMillis);
            dml.setTs(timeMillis);
            dml.setType("INSERT");
            dml.setIsDdl(false);
            dml.setPkNames(pkNames);
            dml.setDatabase(catalog);
            dml.setDestination(destination);
            dmlList.add(dml);
        }
        return dmlList;
    }

    public static void compress(List<Dml> dmlList) {
        if (dmlList.size() <= 1) {
            return;
        }
        Map<Object, Object> cache = new HashMap<>();
        for (Dml dml : dmlList) {
            dml.setMysqlType(cache(dml.getMysqlType(), cache));
            dml.setPkNames(cache(dml.getPkNames(), cache));
            dml.setGroupId(cache(dml.getGroupId(), cache));
            dml.setDatabase(cache(dml.getDatabase(), cache));
            dml.setTable(cache(dml.getTable(), cache));
            dml.setType(cache(dml.getType(), cache));

            compressListMap(dml.getData(), cache);
            compressListMap(dml.getOld(), cache);
        }
    }

    private static <T> T cache(T value, Map<Object, Object> cache) {
        if (value == null) {
            return null;
        }
        return (T) cache.computeIfAbsent(value, k -> k);
    }

    private static void compressListMap(List<Map<String, Object>> data, Map<Object, Object> cache) {
        if (data == null) {
            return;
        }
        for (Map<String, Object> dml : data) {
            if (!dml.isEmpty()) {
                LinkedHashMap<String, Object> copy = new LinkedHashMap<>(dml);
                dml.clear();
                for (Map.Entry<String, Object> entry : copy.entrySet()) {
                    dml.put(cache(entry.getKey(), cache), cache(entry.getValue(), cache));
                }
            }
        }
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

//    public List<String> getPkNames() {
//        return pkNames;
//    }
//
//    public void setPkNames(List<String> pkNames) {
//        this.pkNames = pkNames;
//    }

    public void setTable(String table) {
        this.table = table;
    }

    public Boolean getIsDdl() {
        return isDdl;
    }

    public void setIsDdl(Boolean isDdl) {
        this.isDdl = isDdl;
    }

    public boolean isTypeInit() {
        return "INIT".equalsIgnoreCase(type);
    }

    public boolean isTypeInsert() {
        return "INSERT".equalsIgnoreCase(type);
    }

    public boolean isTypeDelete() {
        return "DELETE".equalsIgnoreCase(type);
    }

    public boolean isTypeUpdate() {
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
