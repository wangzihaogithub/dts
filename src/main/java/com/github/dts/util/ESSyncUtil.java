package com.github.dts.util;

import com.github.dts.impl.elasticsearch.nested.SQL;
import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.SchemaItem.TableItem;
import org.joda.time.DateTime;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ES 同步工具同类
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncUtil {
    private static final Map<String, JdbcTemplate> JDBC_TEMPLATE_MAP = new HashMap<>(3);
    private static final Map<String, String> STRING_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, String> STRING_LRU_CACHE = Collections.synchronizedMap(new LinkedHashMap<String, String>(32, 0.75F, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > 5000;
        }
    });

    public static String trimWhere(String where) {
        if (where == null) {
            return null;
        }
        where = where.trim();
        while (where.startsWith("where")) {
            where = where.substring("where".length());
        }
        while (where.startsWith("WHERE")) {
            where = where.substring("WHERE".length());
        }
        while (where.startsWith("and")) {
            where = where.substring("and".length());
        }
        while (where.startsWith("AND")) {
            where = where.substring("AND".length());
        }
        return where;
    }

    public static Map<String, byte[]> loadYamlToBytes(File configDir) {
        Map<String, byte[]> map = new LinkedHashMap<>();
        // 先取本地文件，再取类路径
        File[] files = configDir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    map.putAll(loadYamlToBytes(file));
                } else {
                    String fileName = file.getName();
                    if (!fileName.endsWith(".yml") && !fileName.endsWith(".yaml")) {
                        continue;
                    }
                    try {
                        byte[] bytes = Files.readAllBytes(file.toPath());
                        map.put(fileName, bytes);
                    } catch (IOException e) {
                        throw new RuntimeException("Read " + configDir + "mapping config: " + fileName + " error. ", e);
                    }
                }
            }
        }
        return map;
    }

    public static String join(Collection list) {
        StringJoiner joiner = new StringJoiner(",");
        for (Object t : list) {
            if (t == null || "".equals(t)) {
                continue;
            }
            joiner.add(t.toString());
        }
        return joiner.toString();
    }

    public static List<String> split(Object str, String separator) {
        if (str == null || "".equals(str)) {
            return Collections.emptyList();
        }
        return Arrays.asList(str.toString().split(separator));
    }

    /**
     * 获取数据源
     *
     * @param srcDataSourcesKey 哪个数据源(配置文件中的key)
     * @return 数据源
     */
    public static JdbcTemplate getJdbcTemplateByKey(String srcDataSourcesKey) {
        JdbcTemplate jdbcTemplate = JDBC_TEMPLATE_MAP.get(srcDataSourcesKey);
        if (jdbcTemplate == null) {
            synchronized (JDBC_TEMPLATE_MAP) {
                jdbcTemplate = JDBC_TEMPLATE_MAP.get(srcDataSourcesKey);
                if (jdbcTemplate == null) {
                    DataSource dataSource = CanalConfig.DatasourceConfig.getDataSource(srcDataSourcesKey);
                    if (dataSource == null) {
                        return null;
                    }
                    jdbcTemplate = new JdbcTemplate(dataSource);
                    JDBC_TEMPLATE_MAP.put(srcDataSourcesKey, jdbcTemplate);
                }
            }
        }

        //如果重新加载配置文件, 那么旧的数据源引用是无效的, 所以这里要判断一下
        if (CanalConfig.DatasourceConfig.contains(jdbcTemplate.getDataSource())) {
            return jdbcTemplate;
        }
        synchronized (JDBC_TEMPLATE_MAP) {
            JDBC_TEMPLATE_MAP.clear();
            jdbcTemplate = new JdbcTemplate(CanalConfig.DatasourceConfig.getDataSource(srcDataSourcesKey));
            JDBC_TEMPLATE_MAP.put(srcDataSourcesKey, jdbcTemplate);
        }
        return jdbcTemplate;
    }

    public static void appendConditionByExpr(StringBuilder sql, Object value, String owner, String columnName, String and) {
        if (owner != null && !owner.isEmpty()) {
            sql.append(owner).append(".");
        }
        sql.append(columnName).append("=").append(value).append(' ');
        sql.append(and);
    }

    public static Map<String, Double> parseGeoPointToMap(String val) {
        String[] point = val.split(",");
        Map<String, Double> location = new HashMap<>(2);
        location.put("lat", Double.parseDouble(point[0].trim()));
        location.put("lon", Double.parseDouble(point[1].trim()));
        return location;
    }

    public static boolean equalsGeoPoint(Map map1, Map map2) {
        Object lat = map1.get("lat");
        Object lat2 = map2.get("lat");
        if (!Objects.equals(String.valueOf(lat), String.valueOf(lat2))) {
            return false;
        }
        Object lon = map1.get("lon");
        Object lon2 = map2.get("lon");
        return Objects.equals(String.valueOf(lon), String.valueOf(lon2));
    }

    public static SQL convertSQLByMapping(ESMapping mapping, Map<String, Object> data,
                                          Map<String, Object> old, TableItem tableItem) {
        StringBuilder sql = new StringBuilder(mapping.getSql() + " WHERE ");
        String alias = tableItem.getAlias();

        List<Object> args = new ArrayList<>();
        for (SchemaItem.FieldItem fkFieldItem : tableItem.getRelationTableFields().keySet()) {
            for (ColumnItem columnItem : fkFieldItem.getColumnItems()) {
                String columnName = columnItem.getColumnName();
                Object value = data.get(columnName);
                if (alias != null) {
                    sql.append(alias).append(".");
                }
                sql.append(columnName).append("=").append('?').append(' ');
                sql.append(" AND ");
                args.add(value);
            }
        }
        int len = sql.length();
        sql.delete(len - " AND ".length(), len);

        LinkedHashMap<String, Object> map = new LinkedHashMap<>(data);
        if (old != null) {
            map.putAll(old);
        }
        return new SQL(sql.toString(), args.toArray(), map);
    }

    public static SQL convertSQLBySubQuery(Map<String, Object> data,
                                           Map<String, Object> old, TableItem tableItem) {
        String alias = tableItem.getAlias();
        StringBuilder sql = new StringBuilder(
                "SELECT * FROM (" + tableItem.getSubQuerySql() + ") " + alias + " WHERE ");

        List<Object> args = new ArrayList<>();
        for (SchemaItem.FieldItem fkFieldItem : tableItem.getRelationTableFields().keySet()) {
            String columnName = fkFieldItem.getColumn().getColumnName();
            Object value = data.get(columnName);
            if (alias != null) {
                sql.append(alias).append(".");
            }
            sql.append(columnName).append("=").append('?').append(' ');
            sql.append(" AND ");
            args.add(value);
        }
        int len = sql.length();
        sql.delete(len - " AND ".length(), len);

        LinkedHashMap<String, Object> map = new LinkedHashMap<>(data);
        if (old != null) {
            map.putAll(old);
        }
        return new SQL(sql.toString(), args.toArray(), map);
    }

    public static SQL convertSqlByMapping(ESMapping mapping, Map<String, Object> data) {
        Set<ColumnItem> idColumns = mapping.getSchemaItem().getGroupByIdColumns();
        TableItem mainTable = mapping.getSchemaItem().getMainTable();

        List<Object> args = new ArrayList<>();
        // 拼接condition
        StringBuilder condition = new StringBuilder(" ");
        for (ColumnItem idColumn : idColumns) {
            Object idVal = data.get(idColumn.getColumnName());
            if (mainTable.getAlias() != null) {
                condition.append(mainTable.getAlias()).append(".");
            }
            condition.append(idColumn.getColumnName()).append("=");
            condition.append('?').append(" AND ");
            args.add(idVal);
        }

        if (condition.toString().endsWith("AND ")) {
            int len2 = condition.length();
            condition.delete(len2 - 4, len2);
        }

        return new SQL(mapping.getSql() + " WHERE " + condition + " ", args.toArray(), data);
    }

    public static String stringCacheLRU(String s) {
        if (s == null) {
            return null;
        }
        return STRING_LRU_CACHE.computeIfAbsent(s, e -> e);
    }

    public static String stringCache(String s) {
        if (s == null) {
            return null;
        }
        return STRING_CACHE.computeIfAbsent(s, e -> e);
    }

    public static boolean isEmpty(Object object) {
        if (object == null) {
            return true;
        }
        if (object instanceof CharSequence && ((CharSequence) object).length() == 0) {
            return true;
        }
        if (object instanceof Collection) {
            return ((Collection) object).isEmpty();
        }
        if (object instanceof Map) {
            return ((Map) object).isEmpty();
        }
        return false;
    }

    private static boolean equalsEsDate(Object mysqlDate, Object esDate) {
        Date dateEs;
        Date dateMysql;
        if (esDate instanceof Date) {
            dateEs = (Date) esDate;
        } else {
            dateEs = DateUtil.parseDate(esDate.toString());
        }
        if (mysqlDate instanceof Date) {
            dateMysql = (Date) mysqlDate;
        } else {
            dateMysql = DateUtil.parseDate(mysqlDate.toString());
        }

        DateTime dateMysqlDateTime = new DateTime(dateMysql);
        DateTime dateEsDateTime = new DateTime(dateEs);
        String format;
        if (dateEsDateTime.getHourOfDay() == 0 && dateEsDateTime.getMinuteOfHour() == 0 && dateEsDateTime.getSecondOfMinute() == 0
                && dateEsDateTime.getMillisOfSecond() == 0) {
            format = "yyyy-MM-dd";
        } else {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        return dateEsDateTime.toString(format).equals(dateMysqlDateTime.toString(format));
    }

    private static boolean equalsValue(Object mysql, Object es) {
        if (mysql == es) {
            return true;
        }
        if (mysql != null && es == null) {
            return false;
        }
        if (mysql == null && es != null) {
            return false;
        }
        if (mysql.getClass().isArray()) {
            if (es instanceof Collection) {
                int mysqlLength = Array.getLength(mysql);
                Collection<?> esColl = ((Collection<?>) es);
                int esLength = esColl.size();
                if (mysqlLength != esLength) {
                    return false;
                }
                Iterator<?> iterator = esColl.iterator();
                for (int i = 0; i < mysqlLength; i++) {
                    Object mysqlValue = Array.get(mysql, i);
                    Object esValue = iterator.next();
                    if (!equalsEsDate(mysqlValue, esValue)) {
                        return false;
                    }
                }
                return true;
            } else if (es.getClass().isArray()) {
                int mysqlLength = Array.getLength(mysql);
                int esLength = Array.getLength(es);
                if (mysqlLength != esLength) {
                    return false;
                }
                for (int i = 0; i < mysqlLength; i++) {
                    Object mysqlValue = Array.get(mysql, i);
                    Object esValue = Array.get(es, i);
                    if (!equalsEsDate(mysqlValue, esValue)) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        } else {
            return Objects.equals(mysql, es);
        }
    }

    public static boolean equalsObjectFieldRowDataValue(Object mysql, Object es, ESSyncConfig.ObjectField objectField, Map<String, Object> mysqlRowData, Map<String, Object> esRowData) {
        ESSyncConfig.ObjectField.Type type = objectField.getType();
        switch (type) {
            case OBJECT_FLAT_SQL://equalsObjectFieldRowDataValue
            case OBJECT_SQL: {//equalsObjectFieldRowDataValue
                return es != null;
            }
            case ARRAY_FLAT_SQL://equalsObjectFieldRowDataValue
            case ARRAY_SQL: {//equalsObjectFieldRowDataValue
                return es != null;
            }
            case STATIC_METHOD: {
                Object mysqlParse = objectField.parse(mysql, objectField.getEsMapping(), mysqlRowData);
                return equalsValue(es, mysqlParse);
            }
            case LLM_VECTOR: {
                boolean mysqlEmpty = mysql == null || "".equals(mysql);
                boolean esEmpty = es == null || "".equals(es);
                if (esEmpty) {
                    return mysqlEmpty;
                } else if (mysqlEmpty) {
                    return false;
                } else {
                    String refTextFieldName = objectField.getParamLlmVector().getEtlEqualsFieldName();
                    if (refTextFieldName == null || refTextFieldName.isEmpty()) {
                        return true;
                    }
                    Object refEs = esRowData.get(refTextFieldName);
                    Object refMysql = mysqlRowData.get(refTextFieldName);
                    return equalsValue(refEs, refMysql);
                }
            }
            case ARRAY: {
                if (es == null && mysql == null) {
                    return true;
                }
                if (es == null || mysql == null) {
                    return false;
                }
                Collection<?> mysqlParse = (Collection) objectField.parse(mysql, objectField.getEsMapping(), mysqlRowData);
                Collection<?> esParse;
                if (es instanceof Collection) {
                    esParse = (Collection) es;
                } else {
                    return false;
                }
                return equalsToStringList(mysqlParse, esParse);
            }
            case BOOLEAN: {
                try {
                    return Objects.equals(TypeUtil.castToBoolean(mysql), TypeUtil.castToBoolean(es));
                } catch (Exception e) {
                    return false;
                }
            }
            default: {
                return equalsValue(mysql, es);
            }
        }
    }

    private static boolean equalsToStringList(Collection<?> mysqlList, Collection<?> esList) {
        if (mysqlList == null && esList == null) {
            return true;
        }
        if (mysqlList == null || esList == null) {
            return false;
        }
        if (mysqlList.size() != esList.size()) {
            return false;
        }
        Iterator<?> mysqlIterator = mysqlList.iterator();
        for (Object es : esList) {
            Object mysql = mysqlIterator.next();
            if (Objects.equals(mysql, es)) {
                continue;
            }
            if (!Objects.equals(
                    Objects.toString(mysql, null),
                    Objects.toString(es, null))) {
                return false;
            }
        }
        return true;
    }

    private static List<Object> flatValue0List(List<Map<String, Object>> rowList) {
        List<Object> list = new ArrayList<>(rowList.size());
        for (Map<String, Object> row : rowList) {
            list.add(EsGetterUtil.value0(row));
        }
        return list;
    }

    public static boolean equalsNestedRowData(List<Map<String, Object>> mysqlRowData, Object esRowData, ESSyncConfig.ObjectField objectField) {
        if (isEmpty(esRowData)) {
            if (mysqlRowData == null || mysqlRowData.isEmpty()) {
                return true;
            } else {
                Map<String, Object> map = mysqlRowData.get(0);
                return map.isEmpty();
            }
        } else if (mysqlRowData == null || mysqlRowData.isEmpty()) {
            return isEmpty(esRowData);
        } else if (objectField.isSqlType()) {
            if (esRowData instanceof Map) {
                Map es = ((Map<?, ?>) esRowData);
                Map<String, Object> mysql = mysqlRowData.get(0);
                for (String field : mysql.keySet()) {
                    Object mysqlValue = mysql.get(field);
                    Object esValue = es.get(field);
                    if (!equalsRowDataValue(mysqlValue, esValue)) {
                        return false;
                    }
                }
                return true;
            } else if (esRowData instanceof Collection) {
                Collection<?> esList = (Collection<?>) esRowData;
                if (esList.size() != mysqlRowData.size()) {
                    return false;
                }
                if (objectField.getType().isFlatSqlType()) {
                    List<Object> mysqlRowFlatData = flatValue0List(mysqlRowData);
                    return equalsToStringList(mysqlRowFlatData, esList);
                } else {
                    Iterator<Map<String, Object>> mysqlIterator = mysqlRowData.iterator();
                    for (Object esObj : esList) {
                        if (!(esObj instanceof Map)) {
                            return false;
                        }
                        Map<String, Object> mysql = mysqlIterator.next();
                        Map<String, Object> es = (Map<String, Object>) esObj;
                        for (String field : mysql.keySet()) {
                            Object mysqlValue = mysql.get(field);
                            Object esValue = es.get(field);
                            ESSyncConfig.ObjectField fieldObjectField = objectField.getEsMapping().getObjectField(objectField.getFieldName(), field);
                            if (fieldObjectField != null) {
                                if (!equalsObjectFieldRowDataValue(mysqlValue, esValue, fieldObjectField, mysql, es)) {
                                    return false;
                                }
                            } else if (!equalsRowDataValue(mysqlValue, esValue)) {
                                return false;
                            }
                        }
                    }
                    return true;
                }
            } else {
                return false;
            }
        } else {
            throw new IllegalArgumentException("unsupported object type: " + objectField.getType());
        }
    }

    public static Collection<String> getRowChangeList(Map<String, Object> mysqlRowData, Map<String, Object> esRowData, Set<String> diffFields, ESMapping esMapping) {
        if (diffFields == null || diffFields.isEmpty()) {
            diffFields = mysqlRowData.keySet();
        }
        Set<String> changeList = new LinkedHashSet<>(Math.max(diffFields.size() / 3, 3));
        for (String diffField : diffFields) {
            ESSyncConfig.ObjectField objectField = esMapping.getObjectField(null, diffField);
            if (objectField != null && objectField.isSqlType()) {
                continue;
            }
            Object mysql = mysqlRowData.get(diffField);
            Object es = esRowData.get(diffField);
            if (objectField != null) {
                if (!equalsObjectFieldRowDataValue(mysql, es, objectField, mysqlRowData, esRowData)) {
                    changeList.add(diffField);
                }
            } else {
                if (!equalsRowDataValue(mysql, es)) {
                    changeList.add(diffField);
                }
            }
        }
        return changeList;
    }

    private static boolean equalsRowDataValue(Object mysql, Object es) {
        boolean emptyMysql = ESSyncUtil.isEmpty(mysql);
        boolean emptyEs = ESSyncUtil.isEmpty(es);
        boolean equals;
        if (emptyMysql != emptyEs) {
            equals = false;
        } else if (emptyMysql) {
            equals = true;
        } else if (mysql instanceof Boolean || es instanceof Boolean) {
            try {
                equals = Objects.equals(TypeUtil.castToBoolean(mysql), TypeUtil.castToBoolean(es));
            } catch (Exception e) {
                equals = false;
            }
        } else if (es instanceof Collection) {
            String mysqlString = mysql.toString();
            for (Object esItem : (Collection) es) {
                if (esItem == null || !mysqlString.contains(esItem.toString())) {
                    return false;
                }
            }
            return true;
        } else if (mysql instanceof Date || es instanceof Date) {
            equals = equalsEsDate(mysql, es);
        } else if (es instanceof Map) {
            Map<String, Double> mysqlGeo = ESSyncUtil.parseGeoPointToMap(mysql.toString());
            equals = ESSyncUtil.equalsGeoPoint(mysqlGeo, (Map) es);
        } else if (es instanceof Integer || es instanceof Long) {
            equals = Long.parseLong(mysql.toString()) == ((Number) es).longValue();
        } else {
            equals = String.valueOf(mysql).equals(String.valueOf(es));
        }
        return equals;
    }

}
