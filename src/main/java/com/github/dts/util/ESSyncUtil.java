package com.github.dts.util;

import com.github.dts.impl.elasticsearch.nested.SQL;
import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.SchemaItem.TableItem;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * ES 同步工具同类
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncUtil {
    private static final Map<String, JdbcTemplate> JDBC_TEMPLATE_MAP = new HashMap<>(3);
    private static final Logger log = LoggerFactory.getLogger(ESSyncUtil.class);
    private static final String[] ES_FORMAT_SUPPORT = {"yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd"};
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

    public static Boolean castToBoolean(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue() == 1;
        } else {
            if (value instanceof String) {
                String strVal = (String) value;
                if (strVal.length() == 0 || "null".equals(strVal) || "NULL".equals(strVal)) {
                    return null;
                }

                if ("true".equalsIgnoreCase(strVal) || "1".equals(strVal)) {
                    return Boolean.TRUE;
                }

                if ("false".equalsIgnoreCase(strVal) || "0".equals(strVal)) {
                    return Boolean.FALSE;
                }

                if ("Y".equalsIgnoreCase(strVal) || "T".equals(strVal)) {
                    return Boolean.TRUE;
                }

                if ("F".equalsIgnoreCase(strVal) || "N".equals(strVal)) {
                    return Boolean.FALSE;
                }
            }

            throw new IllegalStateException("can not cast to boolean, value : " + value);
        }
    }

    /**
     * 类型转换为Mapping中对应的类型
     *
     * @param val             val
     * @param fileName        fileName
     * @param parentFieldName parentFieldName
     * @param esTypes         esTypes
     * @return Mapping中对应的类型
     */
    public static Object typeConvert(Object val, String fileName, ESFieldTypesCache esTypes, String parentFieldName) {
        if (val == null) {
            return null;
        }
        if (esTypes == null) {
            return val;
        }
        Object esType;
        if (parentFieldName != null) {
            Map<String, Object> properties = esTypes.getProperties(parentFieldName, "properties");
            if (properties == null) {
                return val;
            }
            Object typeMap = properties.get(fileName);
            if (typeMap instanceof Map) {
                esType = ((Map<?, ?>) typeMap).get("type");
            } else {
                esType = typeMap;
            }
        } else {
            esType = esTypes.get(fileName);
        }

        Object res = val;
        if ("keyword".equals(esType) || "text".equals(esType)) {
            res = val.toString();
        } else if ("integer".equals(esType)) {
            if (val instanceof Number) {
                res = ((Number) val).intValue();
            } else {
                res = Integer.parseInt(val.toString());
            }
        } else if ("long".equals(esType)) {
            if (val instanceof Number) {
                res = ((Number) val).longValue();
            } else {
                res = Long.parseLong(val.toString());
            }
        } else if ("short".equals(esType)) {
            if (val instanceof Number) {
                res = ((Number) val).shortValue();
            } else {
                res = Short.parseShort(val.toString());
            }
        } else if ("byte".equals(esType)) {
            if (val instanceof Number) {
                res = ((Number) val).byteValue();
            } else {
                res = Byte.parseByte(val.toString());
            }
        } else if ("double".equals(esType)) {
            if (val instanceof Number) {
                res = ((Number) val).doubleValue();
            } else {
                res = Double.parseDouble(val.toString());
            }
        } else if ("float".equals(esType) || "half_float".equals(esType) || "scaled_float".equals(esType)) {
            if (val instanceof Number) {
                res = ((Number) val).floatValue();
            } else {
                res = Float.parseFloat(val.toString());
            }
        } else if ("boolean".equals(esType)) {
            if (val instanceof Boolean) {
                res = val;
            } else if (val instanceof Number) {
                int v = ((Number) val).intValue();
                res = v != 0;
            } else {
                res = Boolean.parseBoolean(val.toString());
            }
        } else if ("date".equals(esType)) {
            if (val instanceof java.sql.Time) {
                DateTime dateTime = new DateTime(((java.sql.Time) val).getTime());
                res = parseDate(fileName, parentFieldName, dateTime, esTypes);
            } else if (val instanceof java.sql.Timestamp) {
                DateTime dateTime = new DateTime(((java.sql.Timestamp) val).getTime());
                res = parseDate(fileName, parentFieldName, dateTime, esTypes);
            } else if (val instanceof java.sql.Date || val instanceof Date) {
                DateTime dateTime;
                if (val instanceof java.sql.Date) {
                    dateTime = new DateTime(((java.sql.Date) val).getTime());
                } else {
                    dateTime = new DateTime(((Date) val).getTime());
                }
                res = parseDate(fileName, parentFieldName, dateTime, esTypes);
            } else if (val instanceof Long) {
                DateTime dateTime = new DateTime(((Long) val).longValue());
                res = parseDate(fileName, parentFieldName, dateTime, esTypes);
            } else if (val instanceof String) {
                String v = ((String) val).trim();
                if (v.length() > 18 && v.charAt(4) == '-' && v.charAt(7) == '-' && v.charAt(10) == ' '
                        && v.charAt(13) == ':' && v.charAt(16) == ':') {
                    String dt = v.substring(0, 10) + "T" + v.substring(11);
                    Date date = Util.parseDate(dt);
                    if (date != null) {
                        DateTime dateTime = new DateTime(date);
                        res = parseDate(fileName, parentFieldName, dateTime, esTypes);
                    }
                } else if (v.length() == 10 && v.charAt(4) == '-' && v.charAt(7) == '-') {
                    Date date = Util.parseDate(v);
                    if (date != null) {
                        DateTime dateTime = new DateTime(date);
                        res = parseDate(fileName, parentFieldName, dateTime, esTypes);
                    }
                } else if (v.length() == 7 && v.charAt(4) == '-') {
                    Date date = Util.parseDate(v);
                    if (date != null) {
                        DateTime dateTime = new DateTime(date);
                        res = parseDate(fileName, parentFieldName, dateTime, esTypes);
                    }
                } else if (v.length() == 4) {
                    Date date = Util.parseDate(v);
                    if (date != null) {
                        DateTime dateTime = new DateTime(date);
                        res = parseDate(fileName, parentFieldName, dateTime, esTypes);
                    }
                }
            }
        } else if ("binary".equals(esType)) {
            if (val instanceof byte[]) {
                res = new String(Base64.getEncoder().encode((byte[]) val), Charset.forName("UTF-8"));
            } else if (val instanceof Blob) {
                byte[] b = blobToBytes((Blob) val);
                res = new String(Base64.getEncoder().encode(b), Charset.forName("UTF-8"));
            } else if (val instanceof String) {
                // 对应canal中的单字节编码
                byte[] b = ((String) val).getBytes(StandardCharsets.ISO_8859_1);
                res = new String(Base64.getEncoder().encode(b), Charset.forName("UTF-8"));
            }
        } else if ("geo_point".equals(esType)) {
            if (!(val instanceof String)) {
//                log.error("es type is geo_point, but source type is not String");
                return val;
            }

            if (!((String) val).contains(",")) {
                log.error("es type is geo_point, source value not contains ',' separator {} {} = {}", parentFieldName, fileName, val);
                return val;
            }
            return parseGeoPointToMap((String) val);
        } else if ("array".equals(esType)) {
            if ("".equals(val.toString().trim())) {
                res = new ArrayList<>();
            } else {
                String value = val.toString();
                String separator = ",";
                if (!value.contains(",")) {
                    if (value.contains(";")) {
                        separator = ";";
                    } else if (value.contains("|")) {
                        separator = "|";
                    } else if (value.contains("-")) {
                        separator = "-";
                    }
                }
                String[] values = value.split(separator);
                return Arrays.asList(values);
            }
        } else if ("object".equals(esType)) {
            if ("".equals(val.toString().trim())) {
                res = new HashMap<>();
            } else {
                res = JsonUtil.toMap(val.toString(), true);
            }
        } else {
            // 其他类全以字符串处理
            res = val.toString();
        }

        return res;
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

    private static byte[] blobToBytes(Blob blob) {
        try (InputStream is = blob.getBinaryStream()) {
            byte[] b = new byte[(int) blob.length()];
            if (is.read(b) != -1) {
                return b;
            } else {
                return new byte[0];
            }
        } catch (IOException | SQLException e) {
            log.error(e.getMessage());
            return null;
        }
    }

    public static SQL convertSQLByMapping(ESMapping mapping, Map<String, Object> data,
                                          Map<String, Object> old, TableItem tableItem) {
        StringBuilder sql = new StringBuilder(mapping.getSql() + " WHERE ");
        String alias = tableItem.getAlias();

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

    public static boolean equalsRowData(Object mysql, Object es, ESSyncConfig.ObjectField objectField, Map<String, Object> mysqlRowData, Map<String, Object> esRowData) {
        ESSyncConfig.ObjectField.Type type = objectField.getType();
        switch (type) {
            case OBJECT_SQL: {
                return es != null;
            }
            case ARRAY_SQL: {
                return es != null;
            }
            case LLM_VECTOR: {
                String refTextFieldName = objectField.getParamLlmVector().getEtlEqualsFieldName();
                if (refTextFieldName == null || refTextFieldName.isEmpty()) {
                    return true;
                }
                Object refEs = esRowData.get(refTextFieldName);
                Object refMysql = mysqlRowData.get(refTextFieldName);
                return Objects.equals(refEs, refMysql);
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
                    esParse = Arrays.asList(es.toString().split(","));
                }
                if (mysqlParse.size() != esParse.size()) {
                    return false;
                }
                Set<String> mysqlParseString = mysqlParse.stream().map(String::valueOf).collect(Collectors.toSet());
                Set<String> esParseString = esParse.stream().map(String::valueOf).collect(Collectors.toSet());
                for (String esString : esParseString) {
                    if (!mysqlParseString.contains(esString)) {
                        return false;
                    }
                }
                return true;
            }
            case BOOLEAN: {
                try {
                    return Objects.equals(castToBoolean(mysql), castToBoolean(es));
                } catch (Exception e) {
                    return false;
                }
            }
            default: {
                return Objects.equals(mysql, es);
            }
        }
    }

    public static List<Map<String, Object>> convertValueTypeCopyList(List<Map<String, Object>> rowList,
                                                                     ESTemplate esTemplate,
                                                                     ESMapping esMapping,
                                                                     String fileName) {
        List<Map<String, Object>> rowListCopy = new ArrayList<>();
        if (rowList != null) {
            for (Map<String, Object> row : rowList) {
                Map<String, Object> rowCopy = new LinkedHashMap<>(row);
                esTemplate.convertValueType(esMapping, fileName, rowCopy);
                rowListCopy.add(rowCopy);
            }
        }
        return rowListCopy;
    }

    public static Map<String, Object> convertValueTypeCopyMap(List<Map<String, Object>> rowList,
                                                              ESTemplate esTemplate,
                                                              ESMapping esMapping,
                                                              String fileName) {
        Map<String, Object> rowCopy;
        if (rowList != null && !rowList.isEmpty()) {
            rowCopy = new LinkedHashMap<>(rowList.get(0));
            esTemplate.convertValueType(esMapping, fileName, rowCopy);
        } else {
            rowCopy = null;
        }
        return rowCopy;
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
        } else if (objectField.getType().isSqlType()) {
            if (esRowData instanceof Map) {
                Map es = ((Map<?, ?>) esRowData);
                Map<String, Object> mysql = mysqlRowData.get(0);
                for (String field : mysql.keySet()) {
                    Object mysqlValue = mysql.get(field);
                    Object esValue = es.get(field);
                    if (!equalsRowData(mysqlValue, esValue)) {
                        return false;
                    }
                }
                return true;
            } else if (esRowData instanceof Collection) {
                List<?> esList = esRowData instanceof List ? (List<?>) esRowData : new ArrayList<>((Collection<?>) esRowData);
                if (esList.size() != mysqlRowData.size()) {
                    return false;
                }
                for (int i = 0, size = mysqlRowData.size(); i < size; i++) {
                    Map<String, Object> mysql = mysqlRowData.get(i);
                    Map<String, Object> es = (Map<String, Object>) esList.get(i);
                    for (String field : mysql.keySet()) {
                        Object mysqlValue = mysql.get(field);
                        Object esValue = es.get(field);
                        ESSyncConfig.ObjectField fieldObjectField = objectField.getEsMapping().getObjectField(objectField.getFieldName(), field);
                        if (fieldObjectField != null) {
                            if (!equalsRowData(mysqlValue, esValue, fieldObjectField, mysql, es)) {
                                return false;
                            }
                        } else if (!equalsRowData(mysqlValue, esValue)) {
                            return false;
                        }
                    }
                }
                return true;
            } else {
                return false;
            }
        } else {
            throw new IllegalArgumentException("unsupported object type: " + objectField.getType());
        }
    }

    public static boolean equalsRowData(Map<String, Object> mysqlRowData, Map<String, Object> esRowData, Set<String> diffFields, ESMapping esMapping) {
        if (diffFields == null || diffFields.isEmpty()) {
            diffFields = mysqlRowData.keySet();
        }
        for (String diffField : diffFields) {
            ESSyncConfig.ObjectField objectField = esMapping.getObjectField(null, diffField);
            if (objectField != null && objectField.getType().isSqlType()) {
                continue;
            }
            Object mysql = mysqlRowData.get(diffField);
            Object es = esRowData.get(diffField);
            if (objectField != null) {
                if (!equalsRowData(mysql, es, objectField, mysqlRowData, esRowData)) {
                    return false;
                }
            } else {
                if (!equalsRowData(mysql, es)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean equalsRowData(Object mysql, Object es) {
        boolean emptyMysql = ESSyncUtil.isEmpty(mysql);
        boolean emptyEs = ESSyncUtil.isEmpty(es);
        boolean equals;
        if (emptyMysql != emptyEs) {
            equals = false;
        } else if (emptyMysql) {
            equals = true;
        } else if (mysql instanceof Boolean || es instanceof Boolean) {
            try {
                equals = Objects.equals(castToBoolean(mysql), castToBoolean(es));
            } catch (Exception e) {
                equals = false;
            }
        } else if (es instanceof Collection) {
            String mysqlString = mysql.toString();
            for (Object esItem : (Collection) es) {
                if (esItem != null && !mysqlString.contains(esItem.toString())) {
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

    private static Object parseDate(String k, String parentFieldName, DateTime dateTime, Map<String, Object> esFieldType) {
        if (esFieldType instanceof ESFieldTypesCache) {
            Set<String> esFormatSet;
            if (parentFieldName != null) {
                esFormatSet = ESFieldTypesCache.parseFormatToSet(((ESFieldTypesCache) esFieldType).getProperties(parentFieldName, "properties", k, "format"));
            } else {
                esFormatSet = ESFieldTypesCache.parseFormatToSet(((ESFieldTypesCache) esFieldType).getProperties(k, "format"));
            }
            if (esFormatSet != null) {
                if (esFormatSet.size() == 1) {
                    return dateTime.toString(esFormatSet.iterator().next());
                }
                for (String format : ES_FORMAT_SUPPORT) {
                    if (esFormatSet.contains(format)) {
                        return dateTime.toString(format);
                    }
                }
                if (esFormatSet.contains("epoch_millis")) {
                    return dateTime.toDate().getTime();
                }
            }
        }
        if (dateTime.getHourOfDay() == 0 && dateTime.getMinuteOfHour() == 0 && dateTime.getSecondOfMinute() == 0
                && dateTime.getMillisOfSecond() == 0) {
            return dateTime.toString("yyyy-MM-dd");
        } else {
            if (dateTime.getMillisOfSecond() != 0) {
                return dateTime.toString("yyyy-MM-dd HH:mm:ss.SSS");
            } else {
                return dateTime.toString("yyyy-MM-dd HH:mm:ss");
            }
        }
    }

    public static Map<String, Object> copyAndConvertType(Map<String, Object> mysqlData, Map<String, Object> esFieldType) {
        if (mysqlData == null) {
            return null;
        }
        if (mysqlData.isEmpty()) {
            return new LinkedHashMap<>();
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : mysqlData.entrySet()) {
            String k = entry.getKey();
            Object val = entry.getValue();
            if (val != null) {
                Object res = val;
                if (val instanceof java.sql.Timestamp) {
                    DateTime dateTime = new DateTime(((java.sql.Timestamp) val).getTime());
                    res = parseDate(k, null, dateTime, esFieldType);
                } else if (val instanceof java.sql.Date || val instanceof Date) {
                    DateTime dateTime;
                    if (val instanceof java.sql.Date) {
                        dateTime = new DateTime(((java.sql.Date) val).getTime());
                    } else {
                        dateTime = new DateTime(((Date) val).getTime());
                    }
                    res = parseDate(k, null, dateTime, esFieldType);
                } else if (val instanceof Map) {
                    res = copyAndConvertType((Map<String, Object>) val, (Map) (esFieldType instanceof ESFieldTypesCache ? ((ESFieldTypesCache) esFieldType).getProperties(k, "properties") : Collections.emptyMap()));
                } else if (val instanceof ArrayList) {
                    List list = (List) val;
                    List reslist = new ArrayList();
                    for (Object temp : list) {
                        if (temp instanceof Map) {
                            reslist.add(copyAndConvertType((Map<String, Object>) temp, esFieldType));
                        } else {
                            reslist.add(temp);
                        }
                    }
                    res = reslist;
                }
                result.put(k, res);
            } else {
                result.put(k, null);
            }
        }
        return result;
    }
}
