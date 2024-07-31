package com.github.dts.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class SQL implements Cloneable {
    public static final Builder DEFAULT_BUILDER = new Builder();
    private static final Logger log = LoggerFactory.getLogger(SQL.class);
    private String packetId;
    private String type;
    /**
     * 全部字段，修改后的
     */
    private Map<String, Object> data;
    /**
     * 修改前字段的数据 update set name = '123'
     * old.keySet() 就是update set的字段名称， 值为改之前的
     */

    private Map<String, Object> old;
    private String database;
    private long logfileOffset;
    private String logfileName;
    private String preparedSql;
    private List<Value> values;
    private int sqlVersion;
    private SQL originalSQL;
    /**
     * 是否丢弃这条sql,默认丢弃
     */
    private boolean discardFlag = true;
    private String table;
    private boolean skipDuplicateKeyFlag;
    private boolean skipTableNoExistFlag;
    private boolean skipUnknownDatabase = true;
    private List<String> pkNames;
    private Timestamp executeTime;
    private String[] destination;                            // 对应canal的实例或者MQ的topic
    private int index;

    public SQL() {

    }

    public SQL(int index, String type, String packetId, Map<String, Object> data, Map<String, Object> old,
               String database, String table, List<String> pkNames, long logfileOffset, String logfileName,
               long executeTime, String[] destination) {
        this.index = index;
        this.packetId = packetId;
        this.type = type;
        this.data = data;
        this.old = old;
        this.database = database;
        this.table = table;
        this.pkNames = pkNames;
        this.logfileOffset = logfileOffset;
        this.logfileName = logfileName;
        this.executeTime = new Timestamp(executeTime);
        this.destination = destination;
    }

    private static <T> T invokeMethod(Object target, String methodName, Class<?>... parameterTypes) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        try {
            Method method = target.getClass().getMethod(methodName, parameterTypes);
            method.setAccessible(true);
            Object result = method.invoke(target);
            return (T) result;
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw e;
        }
    }

    public String getPacketId() {
        return packetId;
    }

    public String getType() {
        return type;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public Map<String, Object> getOld() {
        return old;
    }

    public String getDatabase() {
        return database;
    }

    public long getLogfileOffset() {
        return logfileOffset;
    }

    public String getLogfileName() {
        return logfileName;
    }

    public String getPreparedSql() {
        return preparedSql;
    }

    public List<Value> getValues() {
        return values;
    }

    public int getSqlVersion() {
        return sqlVersion;
    }

    public SQL getOriginalSQL() {
        return originalSQL;
    }

    public boolean isDiscardFlag() {
        return discardFlag;
    }

    public void setDiscardFlag(boolean discardFlag) {
        this.discardFlag = discardFlag;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public boolean isSkipDuplicateKeyFlag() {
        return skipDuplicateKeyFlag;
    }

    public void setSkipDuplicateKeyFlag(boolean skipDuplicateKeyFlag) {
        this.skipDuplicateKeyFlag = skipDuplicateKeyFlag;
    }

    public boolean isSkipTableNoExistFlag() {
        return skipTableNoExistFlag;
    }

    public void setSkipTableNoExistFlag(boolean skipTableNoExistFlag) {
        this.skipTableNoExistFlag = skipTableNoExistFlag;
    }

    public boolean isSkipUnknownDatabase() {
        return skipUnknownDatabase;
    }

    public void setSkipUnknownDatabase(boolean skipUnknownDatabase) {
        this.skipUnknownDatabase = skipUnknownDatabase;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public Timestamp getExecuteTime() {
        return executeTime;
    }

    public String[] getDestination() {
        return destination;
    }

    public int getIndex() {
        return index;
    }

    public <T> T getAfterFields(Class<T> type) {
        return BeanUtil.transform(getAfterFields(), type);
    }

    public Map<String, Object> getAfterFields() {
        switch (type) {
            case "INSERT": {
                return getData();
            }
            case "DELETE": {
                return Collections.emptyMap();
            }
            case "UPDATE": {
                Map<String, Object> map = new LinkedHashMap<>();
                for (Map.Entry<String, Object> entry : getData().entrySet()) {
                    map.put(entry.getKey(), entry.getValue());
                }
                for (Map.Entry<String, Object> entry : getOld().entrySet()) {
                    map.put(entry.getKey(), entry.getValue());
                }
                return map;
            }
            default: {
                return Collections.emptyMap();
            }
        }
    }

    public <T> T getBeforeFields(Class<T> type) {
        return BeanUtil.transform(getBeforeFields(), type);
    }

    public Map<String, Object> getBeforeFields() {
        Map<String, Object> before = getData();
        return before;
    }

    public Integer getTenantId() {
        return getFields("tenant_id", Integer.class);
    }

    public <T> T getFields(String fieldName, Class<T> type) {
        Map<String, Object> old = getOld();
        Map<String, Object> data = getData();
        Object value = null;
        if (old != null) {
            value = old.get(fieldName);
        }
        if (value == null && data != null) {
            value = data.get(fieldName);
        }
        return value == null ? null : TypeUtil.cast(value, type);
    }

    public <T> T getPrimaryKey(String pkName, Class<T> primaryKeyType) {
        Map<String, Object> old = getOld();
        Map<String, Object> data = getData();
        if (old != null && old.containsKey(pkName)) {
            return TypeUtil.cast(old.get(pkName), primaryKeyType);
        } else if (data != null && data.containsKey(pkName)) {
            return TypeUtil.cast(data.get(pkName), primaryKeyType);
        } else {
            return null;
        }
    }

    public Map<String, Object> getChangeBeforeFields() {
        switch (type) {
            case "INSERT": {
                return Collections.emptyMap();
            }
            case "DELETE": {
                return getData();
            }
            case "UPDATE": {
                return getOld();
            }
            default: {
                return Collections.emptyMap();
            }
        }
    }

    public <T> T getChangeBeforeFields(Class<T> type) {
        Map<String, Object> beforeFields = getChangeBeforeFields();
        return BeanUtil.transform(beforeFields, type);
    }

    public <T> T getChangeAfterFields(Class<T> type) {
        Map<String, Object> afterFields = getChangeAfterFields();
        return BeanUtil.transform(afterFields, type);
    }

    public Map<String, Object> getChangeAfterFields() {
        switch (type) {
            case "INSERT": {
                return getData();
            }
            case "DELETE": {
                return Collections.emptyMap();
            }
            case "UPDATE": {
                Map<String, Object> map = new LinkedHashMap<>();
                for (String name : getOld().keySet()) {
                    map.put(name, getData().get(name));
                }
                return map;
            }
            default: {
                return Collections.emptyMap();
            }
        }
    }

    public <T> List<ObjectModifyHistoryUtil.ModifyHistory> getChangeLogList(Class<T> type, int maxDepth, Class<? extends Annotation> logClass) {
        T beforeFields = getChangeBeforeFields(type);
        T afterFields = getChangeAfterFields(type);
        return ObjectModifyHistoryUtil.compare(beforeFields, afterFields, maxDepth, logClass);
    }

    public <T> T getData(Class<T> type) {
        return BeanUtil.transform(data, type);
    }

    public void updateSqlVersion() {
        sqlVersion++;
    }

    public abstract void flushSQL();

    public boolean existPrimaryKey() {
        return getPkNames() != null && !getPkNames().isEmpty();
    }

    public boolean isDdl() {
        return false;
    }

    public boolean isDml() {
        return true;
    }

    public String getId() {
        return logfileName + "_" + logfileOffset;
    }

    @Override
    public SQL clone() {
        try {
            SQL sql = (SQL) super.clone();
            if (this.data != null) {
                sql.data = invokeMethod(this.data, "clone");
            }
            if (this.old != null) {
                sql.old = invokeMethod(this.old, "clone");
            }
            if (this.pkNames != null) {
                sql.pkNames = invokeMethod(this.pkNames, "clone");
            }
            if (this.values != null) {
                sql.values = invokeMethod(this.values, "clone");
            }
            return sql;
        } catch (CloneNotSupportedException | NoSuchMethodException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new IllegalStateException("clone error. error=" + e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        flushSQL();

        if (values == null || values.isEmpty()) {
            return preparedSql;
        }

        Object[] objects = new Object[values.size()];
        for (int i = 0; i < objects.length; i++) {
            Object o = values.get(i).data;
            if (o instanceof Date) {
                o = "'" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(o) + "'";
            } else if (o instanceof Number) {
                o = o.toString();
            } else if (o instanceof byte[]) {
                o = Arrays.toString((byte[]) o);
            } else if (o == null) {
                o = "null";
            } else {
                o = "'" + o.toString() + "'";
            }
            objects[i] = o;
        }
        try {
            String replace = preparedSql.replace("?", "%s");
            return String.format(replace, objects);
        } catch (Exception e) {
            return preparedSql;
        }
    }

    public static class Builder {
        private final InsertSqlBuilder insertSqlBuilder = new InsertSqlBuilder();
        private final UpdateSqlBuilder updateSqlBuilder = new UpdateSqlBuilder();
        private final DeleteSqlBuilder deleteSqlBuilder = new DeleteSqlBuilder();
        private final DdlSqlBuilder ddlSqlBuilder = new DdlSqlBuilder();

        public List<SQL> convert(List<Dml> prototypeContent) {
            if (prototypeContent == null || prototypeContent.isEmpty()) {
                return new ArrayList<>();
            }
            List<SQL> list = new ArrayList<>();
            for (Dml dml : prototypeContent) {
                Collection<SQL> sqlList = convert(dml);
                list.addAll(sqlList);
            }
            return list;
        }

        public List<SQL> convert(Dml dml) {
            if (dml == null) {
                return null;
            }
            boolean isDdl = Boolean.TRUE.equals(dml.getIsDdl());
            Function<Dml, List<SQL>> sqlBuilderFunction;
            String type = dml.getType().toUpperCase();
            switch (type) {
                case "INIT":
                case "INSERT":
                    sqlBuilderFunction = insertSqlBuilder;
                    break;
                case "UPDATE":
                    sqlBuilderFunction = updateSqlBuilder;
                    break;
                case "DELETE":
                    sqlBuilderFunction = deleteSqlBuilder;
                    break;
                default: {
                    if (isDdl) {
                        sqlBuilderFunction = ddlSqlBuilder;
                    } else {
                        sqlBuilderFunction = null;
                        log.warn("No support dml = {}", dml);
                    }
                }
            }
            if (sqlBuilderFunction == null) {
                return null;
            }
            List<SQL> sqlList = sqlBuilderFunction.apply(dml);
            return sqlList;
        }

        public DdlSqlBuilder getDdlSqlBuilder() {
            return ddlSqlBuilder;
        }

        public DeleteSqlBuilder getDeleteSqlBuilder() {
            return deleteSqlBuilder;
        }

        public InsertSqlBuilder getInsertSqlBuilder() {
            return insertSqlBuilder;
        }

        public UpdateSqlBuilder getUpdateSqlBuilder() {
            return updateSqlBuilder;
        }
    }

    public static class DdlSqlBuilder implements Function<Dml, List<SQL>> {
        @Override
        public List<SQL> apply(Dml dml) {
            List<SQL> sqlList = new ArrayList<>(1);
            SQL sql = new DdlSQL(dml.getIndex(), dml.getPacketId(), dml.getType(), SqlParser.trimSchema(dml.getSql()),
                    dml.getDatabase(), dml.getTable(),
                    dml.getPkNames(), dml.getLogfileOffset(), dml.getLogfileName(), dml.getEs(), dml.getDestination());
            sql.originalSQL = sql.clone();
            sqlList.add(sql);
            return sqlList;
        }

        private static class DdlSQL extends SQL {
            DdlSQL(int index, String packetId, String type, String sql, String database, String table, List<String> pkNames, long logfileOffset, String logfileName, long es, String[] destination) {
                super(index, type, packetId, new HashMap<>(), new HashMap<>(), database, table, pkNames, logfileOffset, logfileName, es, destination);
                super.values = new ArrayList<>();
                super.preparedSql = sql;
            }

            @Override
            public boolean isDdl() {
                return true;
            }

            @Override
            public boolean isDml() {
                return false;
            }

            @Override
            public void flushSQL() {

            }
        }
    }

    public static class InsertSqlBuilder implements Function<Dml, List<SQL>> {
        private boolean defaultSkipDuplicateKeyFlag = true;

        public boolean isDefaultSkipDuplicateKeyFlag() {
            return defaultSkipDuplicateKeyFlag;
        }

        public void setDefaultSkipDuplicateKeyFlag(boolean defaultSkipDuplicateKeyFlag) {
            this.defaultSkipDuplicateKeyFlag = defaultSkipDuplicateKeyFlag;
        }

        @Override
        public List<SQL> apply(Dml dml) {
            List<Map<String, Object>> dataList = dml.getData();
            if (dataList == null || dataList.isEmpty()) {
                return Collections.emptyList();
            }
            List<SQL> sqlList = new ArrayList<>(dataList.size());
            for (Map<String, Object> data : dataList) {
                SQL sql = new InsertSQL(dml.getIndex(), dml.getPacketId(), data, null,
                        dml.getDatabase(), dml.getTable(),
                        dml.getPkNames(), dml.getLogfileOffset(), dml.getLogfileName(), dml.getEs(), dml.getDestination());

                sql.setSkipDuplicateKeyFlag(defaultSkipDuplicateKeyFlag);
                sql.originalSQL = sql.clone();
                sqlList.add(sql);
            }
            return sqlList;
        }

        private static class InsertSQL extends SQL {
            private final StringBuilder sqlBuilder = new StringBuilder();
            private int flushVersion;

            InsertSQL(int index, String packetId, Map<String, Object> data, Map<String, Object> old, String database, String table, List<String> pkNames, long logfileOffset, String logfileName, long es, String[] destination) {
                super(index, "INSERT", packetId, data, old, database, table, pkNames, logfileOffset, logfileName, es, destination);
            }

            @Override
            public void flushSQL() {
                Map<String, Object> data = getData();
                if (data == null || data.isEmpty()) {
                    setDiscardFlag(true);
                    return;
                }
                int sqlVersion = getSqlVersion();
                if (sqlVersion > 0 && flushVersion == sqlVersion) {
                    return;
                }
                flushVersion = sqlVersion;
                sqlBuilder.setLength(0);
                List<Value> valueList = new ArrayList<>();

                List<String> skipKeys = data.keySet().stream()
                        .filter(e -> e.startsWith("#alibaba_rds"))
                        .collect(Collectors.toList());
                data = new LinkedHashMap<>(data);
                for (String skipKey : skipKeys) {
                    data.remove(skipKey);
                }
                sqlBuilder.append("INSERT INTO ").append(getTable()).append(" (`")
                        .append(String.join("`,`", data.keySet())).append("`) VALUES (");
                IntStream.range(0, data.size()).forEach(j -> sqlBuilder.append("?,"));
                sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length()).append(")");

                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    valueList.add(new Value(entry.getKey(), entry.getValue()));
                }
                super.values = valueList;
                super.preparedSql = sqlBuilder.toString();
            }
        }
    }

    public static class UpdateSqlBuilder implements Function<Dml, List<SQL>> {
        private boolean defaultSkipDuplicateKeyFlag = true;

        public boolean isDefaultSkipDuplicateKeyFlag() {
            return defaultSkipDuplicateKeyFlag;
        }

        public void setDefaultSkipDuplicateKeyFlag(boolean defaultSkipDuplicateKeyFlag) {
            this.defaultSkipDuplicateKeyFlag = defaultSkipDuplicateKeyFlag;
        }

        @Override
        public List<SQL> apply(Dml dml) {
            List<Map<String, Object>> dataList = dml.getData();
            List<Map<String, Object>> oldList = dml.getOld();
            if (dataList == null || dataList.isEmpty() || oldList == null || oldList.isEmpty()) {
                return Collections.emptyList();
            }

            List<SQL> sqlList = new ArrayList<>(dataList.size());
            for (int i = 0, size = dataList.size(); i < size; i++) {
                Map<String, Object> data = dataList.get(i);
                Map<String, Object> old = oldList.get(i);

                //拼接SET字段
                SQL sql = new UpdateSQL(dml.getIndex(), dml.getPacketId(), data, old, dml.getDatabase(), dml.getTable(),
                        dml.getPkNames(), dml.getLogfileOffset(), dml.getLogfileName(), dml.getEs(), dml.getDestination());
                sql.setSkipDuplicateKeyFlag(defaultSkipDuplicateKeyFlag);
                sql.originalSQL = sql.clone();
                sqlList.add(sql);
            }
            return sqlList;
        }

        private static class UpdateSQL extends SQL {
            private final StringBuilder sqlBuilder = new StringBuilder();
            private int flushVersion;

            UpdateSQL(int index, String packetId, Map<String, Object> data, Map<String, Object> old, String database, String table, List<String> pkNames, long logfileOffset, String logfileName, long es, String[] destination) {
                super(index, "UPDATE", packetId, data, old, database, table, pkNames, logfileOffset, logfileName, es, destination);
            }

            @Override
            public void flushSQL() {
                Map<String, Object> old = getOld();
                if (old == null || old.isEmpty()) {
                    setDiscardFlag(true);
                }
                int sqlVersion = getSqlVersion();
                if (sqlVersion > 0 && flushVersion == sqlVersion) {
                    return;
                }
                flushVersion = sqlVersion;
                sqlBuilder.setLength(0);
                List<Value> valueList = new ArrayList<>();

                sqlBuilder.append("UPDATE ").append(getTable()).append(" SET ");
                for (Map.Entry<String, Object> entry : getOld().entrySet()) {
                    sqlBuilder.append('`').append(entry.getKey()).append('`').append("=?, ");
                    valueList.add(new Value(entry.getKey(), getData().get(entry.getKey())));
                }

                int len = sqlBuilder.length();
                sqlBuilder.delete(len - 2, len).append(" WHERE ");

                // 拼接主键
                List<String> pkNames = getPkNames();
                if (pkNames != null) {
                    for (String targetColumnName : pkNames) {
                        sqlBuilder.append(targetColumnName).append("=? AND ");
                        //如果改ID的话
                        Object pk;
                        if (getOld().containsKey(targetColumnName)) {
                            pk = getOld().get(targetColumnName);
                        } else {
                            pk = getData().get(targetColumnName);
                        }
                        valueList.add(new Value(targetColumnName, pk));
                    }
                    sqlBuilder.delete(sqlBuilder.length() - 4, sqlBuilder.length());
                }
                super.values = valueList;
                super.preparedSql = sqlBuilder.toString();
            }

        }
    }

    public static class DeleteSqlBuilder implements Function<Dml, List<SQL>> {
        private boolean defaultSkipDuplicateKeyFlag = true;

        public boolean isDefaultSkipDuplicateKeyFlag() {
            return defaultSkipDuplicateKeyFlag;
        }

        public void setDefaultSkipDuplicateKeyFlag(boolean defaultSkipDuplicateKeyFlag) {
            this.defaultSkipDuplicateKeyFlag = defaultSkipDuplicateKeyFlag;
        }

        @Override
        public List<SQL> apply(Dml dml) {
            List<Map<String, Object>> dataList = dml.getData();
            if (dataList == null || dataList.isEmpty()) {
                return Collections.emptyList();
            }

            List<SQL> sqlList = new ArrayList<>(dataList.size());
            for (Map<String, Object> data : dataList) {
                SQL sql = new DeleteSQL(dml.getIndex(), dml.getPacketId(), data, null, dml.getDatabase(), dml.getTable(),
                        dml.getPkNames(), dml.getLogfileOffset(), dml.getLogfileName(), dml.getEs(), dml.getDestination());
                sql.setSkipDuplicateKeyFlag(defaultSkipDuplicateKeyFlag);
                sql.originalSQL = sql.clone();
                sqlList.add(sql);
            }
            return sqlList;
        }

        private static class DeleteSQL extends SQL {
            private final StringBuilder sqlBuilder = new StringBuilder();
            private int flushVersion;

            DeleteSQL(int index, String packetId, Map<String, Object> data, Map<String, Object> old, String database, String table, List<String> pkNames, long logfileOffset, String logfileName, long es, String[] destination) {
                super(index, "DELETE", packetId, data, old, database, table, pkNames, logfileOffset, logfileName, es, destination);
            }

            @Override
            public void flushSQL() {
                List<String> pkNames = getPkNames();
                if (pkNames == null || pkNames.isEmpty()) {
                    setDiscardFlag(true);
                }
                int sqlVersion = getSqlVersion();
                if (sqlVersion > 0 && flushVersion == sqlVersion) {
                    return;
                }
                flushVersion = sqlVersion;
                sqlBuilder.setLength(0);
                List<Value> valueList = new ArrayList<>();
                sqlBuilder.append("DELETE FROM ").append(getTable()).append(" WHERE ");

                // 拼接主键
                if (pkNames != null) {
                    for (String targetColumnName : pkNames) {
                        sqlBuilder.append('`').append(targetColumnName).append('`').append("=? AND ");
                        valueList.add(new Value(targetColumnName, getData().get(targetColumnName)));
                    }
                    sqlBuilder.delete(sqlBuilder.length() - 4, sqlBuilder.length());
                }

                super.values = valueList;
                super.preparedSql = sqlBuilder.toString();
            }
        }
    }

    public static class Value {
        private String columnName;
        private Object data;

        public Value(String columnName, Object data) {
            this.columnName = columnName;
            this.data = data;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public Object getData() {
            return data;
        }

        public void setData(Object data) {
            this.data = data;
        }

        @Override
        public String toString() {
            return String.valueOf(data);
        }
    }
}
