package com.github.dts.impl.rds;

import com.github.dts.util.AbstractMessageService;
import com.github.dts.util.DateUtil;
import com.github.dts.util.ExpiryLRUMap;
import com.github.dts.util.SQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.Date;

/**
 * 关系型数据库SQL消费者
 *
 * @author wangzihao
 */
public class RDSWriter {
    private static final Logger log = LoggerFactory.getLogger(RDSWriter.class);
    private final ExpiryLRUMap<String, Map<String, Integer>> columnsTypeCacheMap
            = new ExpiryLRUMap<>(1000 * 60 * 5);//写入库表字段类型缓存: instance.schema.table -> < columnName, jdbcType>
    private final Map<String, Set<String>> tableHistoryMap = new LinkedHashMap<>();
    private final Map<String, ErrorSqlRetry> errorSqlRetryMap = new LinkedHashMap<>();
    private AbstractMessageService messageService = AbstractMessageService.LogMessageService.INSTANCE;
    private final DataSource writerDataSource;
    private String catalog;
    /**
     * 错误报告发送间隔(10分钟)
     */
    private int errorReportSendIntervalMinute = 10;

    public Map<String, Set<String>> getTableHistoryMap() {
        return tableHistoryMap;
    }

    public void setColumnsTypeCacheExpiryTimeMs(long columnsTypeCacheExpiryTimeMs) {
        columnsTypeCacheMap.setDefaultExpiryTime(columnsTypeCacheExpiryTimeMs);
    }

    public long getColumnsTypeCacheExpiryTimeMs() {
        return columnsTypeCacheMap.getDefaultExpiryTime();
    }

    public int getErrorReportSendIntervalMinute() {
        return errorReportSendIntervalMinute;
    }

    public void setErrorReportSendIntervalMinute(int errorReportSendIntervalMinute) {
        this.errorReportSendIntervalMinute = errorReportSendIntervalMinute;
    }

    public RDSWriter(DataSource writerDataSource) {
        this.writerDataSource = writerDataSource;
    }

    /**
     * 设置 preparedStatement
     *
     * @param type  sqlType
     * @param pstmt 需要设置的preparedStatement
     * @param value 值
     * @param i     索引号
     */
    private static void setPreparedStatementArgs(Integer type, PreparedStatement pstmt, Object value, int i) throws SQLException {
        if (type == null || value instanceof byte[]) {
            pstmt.setObject(i, value);
            return;
        }

        switch (type) {
            case Types.BOOLEAN:
                if (value instanceof Boolean) {
                    pstmt.setBoolean(i, (Boolean) value);
                } else if (value instanceof String) {
                    boolean v = !"0".equals(value);
                    pstmt.setBoolean(i, v);
                } else if (value instanceof Number) {
                    boolean v = ((Number) value).intValue() != 0;
                    pstmt.setBoolean(i, v);
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                if (value instanceof String) {
                    pstmt.setString(i, (String) value);
                } else if (value == null) {
                    pstmt.setNull(i, type);
                } else {
                    pstmt.setString(i, value.toString());
                }
                break;
            case Types.BIT:
            case Types.TINYINT:
                if (value instanceof Number) {
                    pstmt.setByte(i, ((Number) value).byteValue());
                } else if (value instanceof String) {
                    pstmt.setByte(i, Byte.parseByte((String) value));
                } else if (value instanceof Boolean) {
                    pstmt.setByte(i, (byte) (Boolean.TRUE.equals(value) ? 1 : 0));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.SMALLINT:
                if (value instanceof Number) {
                    pstmt.setShort(i, ((Number) value).shortValue());
                } else if (value instanceof String) {
                    pstmt.setShort(i, Short.parseShort((String) value));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.INTEGER:
                if (value instanceof Number) {
                    pstmt.setInt(i, ((Number) value).intValue());
                } else if (value instanceof String) {
                    pstmt.setInt(i, Integer.parseInt((String) value));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.BIGINT:
                if (value instanceof Number) {
                    pstmt.setLong(i, ((Number) value).longValue());
                } else if (value instanceof String) {
                    pstmt.setLong(i, Long.parseLong((String) value));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (value instanceof BigDecimal) {
                    pstmt.setBigDecimal(i, (BigDecimal) value);
                } else if (value instanceof Byte) {
                    pstmt.setInt(i, ((Byte) value).intValue());
                } else if (value instanceof Short) {
                    pstmt.setInt(i, ((Short) value).intValue());
                } else if (value instanceof Integer) {
                    pstmt.setInt(i, (Integer) value);
                } else if (value instanceof Long) {
                    pstmt.setLong(i, (Long) value);
                } else if (value instanceof Float) {
                    pstmt.setBigDecimal(i, new BigDecimal(String.valueOf(value)));
                } else if (value instanceof Double) {
                    pstmt.setBigDecimal(i, new BigDecimal(String.valueOf(value)));
                } else if (value != null) {
                    pstmt.setBigDecimal(i, new BigDecimal(value.toString()));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.REAL:
                if (value instanceof Number) {
                    pstmt.setFloat(i, ((Number) value).floatValue());
                } else if (value instanceof String) {
                    pstmt.setFloat(i, Float.parseFloat((String) value));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                if (value instanceof Number) {
                    pstmt.setDouble(i, ((Number) value).doubleValue());
                } else if (value instanceof String) {
                    pstmt.setDouble(i, Double.parseDouble((String) value));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                if (value instanceof Blob) {
                    pstmt.setBlob(i, (Blob) value);
                } else if (value instanceof byte[]) {
                    pstmt.setBytes(i, (byte[]) value);
                } else if (value instanceof String) {
                    pstmt.setBytes(i, ((String) value).getBytes(StandardCharsets.ISO_8859_1));
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.CLOB:
                if (value instanceof Clob) {
                    pstmt.setClob(i, (Clob) value);
                } else if (value instanceof byte[]) {
                    pstmt.setBytes(i, (byte[]) value);
                } else if (value instanceof String) {
                    Reader clobReader = new StringReader((String) value);
                    pstmt.setCharacterStream(i, clobReader);
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.DATE:
                if (value instanceof java.sql.Date) {
                    pstmt.setDate(i, (java.sql.Date) value);
                } else if (value instanceof Date) {
                    pstmt.setDate(i, new java.sql.Date(((Date) value).getTime()));
                } else if (value instanceof String) {
                    String v = (String) value;
                    if (!v.startsWith("0000-00-00")) {
                        Date date = DateUtil.parseDate(v);
                        if (date != null) {
                            pstmt.setDate(i, new java.sql.Date(date.getTime()));
                        } else {
                            pstmt.setNull(i, type);
                        }
                    } else {
                        pstmt.setObject(i, value);
                    }
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.TIME:
                if (value instanceof Time) {
                    pstmt.setTime(i, (Time) value);
                } else if (value instanceof Date) {
                    pstmt.setTime(i, new Time(((Date) value).getTime()));
                } else if (value instanceof String) {
                    String v = (String) value;
                    Date date = DateUtil.parseDate(v);
                    if (date != null) {
                        pstmt.setTime(i, new Time(date.getTime()));
                    } else {
                        pstmt.setNull(i, type);
                    }
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            case Types.TIMESTAMP:
                if (value instanceof Timestamp) {
                    pstmt.setTimestamp(i, (Timestamp) value);
                } else if (value instanceof Date) {
                    pstmt.setTimestamp(i, new Timestamp(((Date) value).getTime()));
                } else if (value instanceof String) {
                    String v = (String) value;
                    if (!v.startsWith("0000-00-00")) {
                        Date date = DateUtil.parseDate(v);
                        if (date != null) {
                            pstmt.setTimestamp(i, new Timestamp(date.getTime()));
                        } else {
                            pstmt.setNull(i, type);
                        }
                    } else {
                        pstmt.setObject(i, value);
                    }
                } else {
                    pstmt.setNull(i, type);
                }
                break;
            default:
                pstmt.setObject(i, value, type);
        }
    }

    private void addHistoryLog(String database, String table) {
        if (table == null) {
            return;
        }
        if (table.contains("temp_table")) {
            return;
        }
        tableHistoryMap.computeIfAbsent(database, s -> new LinkedHashSet<>()).add(table);
    }

    public void setMessageService(AbstractMessageService messageService) {
        this.messageService = Objects.requireNonNull(messageService, "messageService");
    }

    public AbstractMessageService getMessageService() {
        return messageService;
    }

    public void write(List<SQL> sqlList) throws SQLException {
        if (sqlList == null || sqlList.isEmpty()) {
            return;
        }
        DataSource dataSource = this.writerDataSource;
        if (dataSource == null) {
            return;
        }

        int sqlIndex = 0;
        SQL currentSql = null;
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            for (SQL sql : sqlList) {
                sqlIndex++;
                currentSql = sql;
                sql.flushSQL();
                if (log.isInfoEnabled()) {
                    log.info("accept sql[{}] discard={}, sql={}", sqlIndex, sql.isDiscardFlag(), sql);
                }
                if (sql.isDiscardFlag()) {
                    continue;
                }

                addHistoryLog(sql.getDatabase(), sql.getTable());

                Map<String, Integer> columnTypeMap = null;
                if (!sql.getValues().isEmpty()) {
                    columnTypeMap = getColumnTypeMap(dataSource, sql.getDatabase(), sql.getTable());
                }
                try {
                    PreparedStatement preparedStatement = connection.prepareStatement(sql.getPreparedSql());
                    int i = 1;
                    for (SQL.Value value : sql.getValues()) {
                        Integer columnType = columnTypeMap == null ? null : columnTypeMap.get(value.getColumnName().toLowerCase());
                        setPreparedStatementArgs(columnType, preparedStatement, value.getData(), i++);
                    }
                    preparedStatement.execute();
                    preparedStatement.close();
                } catch (SQLException e) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw e;
                    }
                    SQLException warpException = handleSQLException(e, currentSql);
                    if (warpException != null) {
                        connection.rollback();
                        sendErrorReport(currentSql, e);
                        throw warpException;
                    }
                }
            }
            log.info("connection.commit()");
            connection.commit();
        } catch (SQLException e) {
            if (Thread.currentThread().isInterrupted()) {
                throw e;
            }
            SQLException wrapException = handleSQLException(e, currentSql);
            if (wrapException != null) {
                throw wrapException;
            }
        } finally {
            log.debug("table history {}", tableHistoryMap);
        }
        sendRecoveryReport(sqlList, new Timestamp(System.currentTimeMillis()));
    }

    private void sendRecoveryReport(List<SQL> sqlList, Timestamp now) {
        if (errorSqlRetryMap.isEmpty()) {
            return;
        }

        for (SQL currentSql : sqlList) {
            ErrorSqlRetry sqlRetry = errorSqlRetryMap.remove(currentSql.getId());
            if (sqlRetry == null) {
                continue;
            }
            String catalog = getCatalog();
            String content = "来源库 = " + currentSql.getDatabase()
                    + ",\n\n 目标库 = " + catalog
                    + ",\n\n 源库执行日期 = " + currentSql.getExecuteTime()
                    + ",\n\n 目标执行日期 = " + now
                    + ",\n\n 重试次数 = " + sqlRetry.retryCount
                    + ",\n\n 订阅 = " + currentSql.getDestination()
                    + ",\n\n packetId = " + currentSql.getPacketId()
                    + ",\n\n 类型 = " + currentSql.getType()
                    + ",\n\n 表 = " + currentSql.getTable()
                    + ",\n\n 日志文件 = " + currentSql.getLogfileName()
                    + ",\n\n 日志偏移 = " + currentSql.getLogfileOffset()
                    + ",\n\n SQL = " + currentSql
                    + "\n\n 同步历史 = " + tableHistoryMap;
            messageService.send("数据库同步已恢复", content);
        }
    }

    private void sendErrorReport(SQL currentSql, SQLException e) {
        String id = currentSql.getId();
        ErrorSqlRetry exist = errorSqlRetryMap.get(id);
        if (exist != null) {
            exist.retryCount++;
            if (exist.isArrivalSendTime()) {
                log.info("sendReport arrivalSendTime. retryCount = {} ", exist.retryCount);
                exist.sendReport();
            } else {
                log.info("skip sendReport cause exist. retryCount = {} ", exist.retryCount);
            }
            return;
        }

        String catalog = getCatalog();
        StringWriter writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));
        String content = "来源库 = " + currentSql.getDatabase()
                + ",\n\n 目标库 = " + catalog
                + ",\n\n 源库执行日期 = " + currentSql.getExecuteTime()
                + ",\n\n 订阅 = " + Arrays.toString(currentSql.getDestination())
                + ",\n\n packetId = " + currentSql.getPacketId()
                + ",\n\n 类型 = " + currentSql.getType()
                + ",\n\n 表 = " + currentSql.getTable()
                + ",\n\n 日志文件 = " + currentSql.getLogfileName()
                + ",\n\n 日志偏移 = " + currentSql.getLogfileOffset()
                + ",\n\n 错误码 = " + e.getSQLState()
                + ",\n\n 错误信息 = " + e
                + ",\n\n SQL = " + currentSql
                + ",\n\n 同步历史 = " + tableHistoryMap
                + " \n\n  ---  "
                + "\n\n 错误明细 = " + writer;

        ErrorSqlRetry retry = new ErrorSqlRetry(currentSql) {
            @Override
            public void sendReport() {
                nextSendReportTime = Timestamp.from(Instant.now().plusMillis(errorReportSendIntervalMinute));
                String name = retryCount == 0 ? "首次发送" : "第" + retryCount + "次重试";
                messageService.send("数据库同步异常(" + name + ")", content);
            }
        };
        errorSqlRetryMap.put(id, retry);
        log.info("sendReport first.  ");
        retry.sendReport();
    }

    public String getCatalog() {
        if (catalog != null) {
            return catalog;
        }
        try (Connection connection = writerDataSource.getConnection()) {
            return this.catalog = connection.getCatalog();
        } catch (SQLException ignored) {
        }
        return null;
    }

    /**
     * 获取目标字段类型
     *
     * @return 字段sqlType
     */
    private Map<String, Integer> getColumnTypeMap(DataSource dataSource, String database, String table) throws SQLException {
        if (dataSource == null || table == null || table.isEmpty()) {
            return Collections.emptyMap();
        }
        String cacheKey = database + "." + table;
        Map<String, Integer> columnType = columnsTypeCacheMap.get(cacheKey);
        if (columnType == null) {
            synchronized (columnsTypeCacheMap) {
                columnType = columnsTypeCacheMap.get(cacheKey);
                if (columnType == null) {
                    columnType = new LinkedHashMap<>();
                    try (Connection connection = dataSource.getConnection();
                         Statement stmt = connection.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " WHERE 1=2")) {
                        ResultSetMetaData rsd = rs.getMetaData();
                        int columnCount = rsd.getColumnCount();
                        for (int i = 1; i <= columnCount; i++) {
                            columnType.put(rsd.getColumnName(i).toLowerCase(), rsd.getColumnType(i));
                        }
                    }
                    columnsTypeCacheMap.put(cacheKey, columnType);
                }
            }
        }
        return columnType;
    }

    private SQLException handleSQLException(SQLException e, SQL sql) {
        if (isSkipException(e, sql)) {
            return null;
        } else {
            return e;
//            new DelayRetryException("SQLException. sql=" + sql + ", error=" + e, e);
        }
    }

    private boolean isSkipException(SQLException e, SQL sql) {
        if ("talent_consultant_info".equals(sql.getTable())) {
            return true;
        }
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        boolean isDuplicateKey = message.contains("Duplicate entry")
                || message.contains("Duplicate key name")
                || message.startsWith("ORA-00001: 违反唯一约束条件");
        if (isDuplicateKey) {
            log.warn("Skip SQLException. sql={}, error={}", sql, e.toString(), e);
            return sql.isSkipDuplicateKeyFlag();
        }

        boolean isSkipUnknownDatabase = message.startsWith("Unknown database");
        if (isSkipUnknownDatabase) {
            log.warn("Skip UnknownDatabase. sql={}, error={}", sql, e.toString(), e);
            return sql.isSkipTableNoExistFlag();
        }

        boolean isSkipTableNoExist = message.matches("Table '.*' doesn't exist.*")
                || message.startsWith("Unknown table") || message.matches(".*Can't DROP '.*'.*");
        if (isSkipTableNoExist) {
            return sql.isSkipTableNoExistFlag();
        }
        return false;
    }

    @Override
    public String toString() {
        return "{" + getClass().getSimpleName() + ",writerDataSource=" + writerDataSource + "}";
    }

    public static abstract class ErrorSqlRetry {
        protected final Date beginTime = new Timestamp(System.currentTimeMillis());
        protected SQL sql;
        protected int retryCount = 0;
        protected Date nextSendReportTime;

        public ErrorSqlRetry(SQL sql) {
            this.sql = sql;
        }

        public abstract void sendReport();

        public Date getBeginTime() {
            return beginTime;
        }

        public SQL getSql() {
            return sql;
        }

        public void setSql(SQL sql) {
            this.sql = sql;
        }

        public int getRetryCount() {
            return retryCount;
        }

        public void setRetryCount(int retryCount) {
            this.retryCount = retryCount;
        }

        public Date getNextSendReportTime() {
            return nextSendReportTime;
        }

        public void setNextSendReportTime(Date nextSendReportTime) {
            this.nextSendReportTime = nextSendReportTime;
        }

        /**
         * 是否到达下一次发送时间
         *
         * @return true=到时间了,false=没到
         */
        public boolean isArrivalSendTime() {
            return System.currentTimeMillis() >= nextSendReportTime.getTime();
        }
    }
}
