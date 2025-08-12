package com.github.dts.util;

import com.github.dts.impl.elasticsearch.nested.SQL;
import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.SchemaItem.TableItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/**
 * ES 同步工具同类
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncUtil {
    private static final Logger log = LoggerFactory.getLogger(ESSyncUtil.class);
    private static final Map<String, JdbcTemplate> JDBC_TEMPLATE_MAP = new HashMap<>(3);
    private static final Map<String, String> STRING_CACHE = Util.newComputeIfAbsentMap(32, 0.75F, true, 1000);
    private static final Map<String, String> STRING_LRU_CACHE = Util.newComputeIfAbsentMap(32, 0.75F, true, 5000);
    private static final Map<String, Map<String, byte[]>> LOAD_YAML_TO_BYTES_CACHE = Util.newComputeIfAbsentMap(32, 0.75F, true, 30);

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

    public static Map<String, byte[]> loadYamlToBytes(JarFile jarFile, String rootEntryName) {
        Map<String, byte[]> map = new LinkedHashMap<>();
        if (jarFile == null) {
            return map;
        }
        try {
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String entryName = entry.getName();
                // 判断是否为 YAML 文件
                if (!entryName.startsWith(rootEntryName) || !isYaml(entryName)) {
                    continue;
                }
                // 忽略目录条目（无需递归处理，因为每个文件都是扁平化条目）
                if (entry.isDirectory()) {
                    continue;
                }
                log.info("loadYamlToBytes jar file = {}", entryName);
                try (InputStream is = jarFile.getInputStream(entry)) {
                    byte[] bytes = readStream(is);
                    if (bytes.length > 0) {
                        String fileName = extractFileName(entryName);
                        map.put(fileName, bytes);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Read " + entryName + " error." + e, e);
                }
            }
        } finally {
            try {
                jarFile.close();
            } catch (IOException ignored) {

            }
        }
        return map;
    }

    private static boolean isYaml(String fileName) {
        return fileName.endsWith(".yml") || fileName.endsWith(".yaml");
    }

    // 从路径中提取文件名（去掉路径部分）
    private static String extractFileName(String entryName) {
        int lastSlashIndex = entryName.lastIndexOf('/');
        return lastSlashIndex >= 0 ? entryName.substring(lastSlashIndex + 1) : entryName;
    }

    // 读取 InputStream 到 byte[]
    private static byte[] readStream(InputStream is) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[Math.max(is.available(), 16384)];
        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        return buffer.toByteArray();
    }

    public static void main(String[] args) throws IOException {
        String userHome = System.getProperty("user.home");
        String jarPath = userHome + "/Desktop/cnwy-dts.jar";
//        Map<String, byte[]> stringMap = loadYamlToBytes(new URL("jar:file://" + jarPath));
        URL url = new URL("jar:file:/" + jarPath + "/cnwy-dts.jar!/BOOT-INF/classes!/es");
        Map<String, byte[]> stringMap = loadYamlToBytes(url);
        System.out.println("stringMap = " + stringMap);
    }

    public static Map<String, byte[]> loadYamlToBytes(URL configDir) {
        Map<String, byte[]> stringMap = LOAD_YAML_TO_BYTES_CACHE.computeIfAbsent(configDir.toString(), unused -> {
            if ("jar".equals(configDir.getProtocol())) {
                try {
                    JarURLConnection connection = (JarURLConnection) configDir.openConnection();
                    return loadYamlToBytes(connection.getJarFile(), connection.getEntryName());
                } catch (IOException e) {
                    throw new RuntimeException("Read config: " + configDir + " error. " + e.toString(), e);
                }
            } else {
                String path = configDir.getPath();
                File dir = new File(path);
                return loadYamlToBytes(dir);
            }
        });
        return new LinkedHashMap<>(stringMap);
    }

    public static Map<String, byte[]> loadYamlToBytes(File dir) {
        Map<String, byte[]> map = new LinkedHashMap<>();
        // 先取本地文件，再取类路径
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    map.putAll(loadYamlToBytes(file));
                } else {
                    String fileName = file.getName();
                    if (!isYaml(fileName)) {
                        continue;
                    }
                    log.info("loadYamlToBytes dir file = {}, dir = {}", fileName, dir);
                    try {
                        byte[] bytes = Files.readAllBytes(file.toPath());
                        if (bytes.length > 0) {
                            map.put(fileName, bytes);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("Read " + dir + "mapping config: " + fileName + " error. ", e);
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
        String esStringDate;
        String mysqlStringDate;
        String stringDateFormat = "yyyy-MM-dd HH:mm:ss.SSS";
        if (esDate instanceof String) {
            esStringDate = (String) esDate;
        } else if (esDate instanceof Date) {
            esStringDate = DateUtil.dateFormat((Date) esDate, stringDateFormat);
        } else {
            esStringDate = "";
        }
        if (mysqlDate instanceof String) {
            mysqlStringDate = (String) mysqlDate;
        } else if (mysqlDate instanceof Date) {
            mysqlStringDate = DateUtil.dateFormat((Date) mysqlDate, stringDateFormat);
        } else {
            mysqlStringDate = "";
        }
        if (mysqlStringDate.length() < esStringDate.length()) {
            String substring = esStringDate.substring(0, mysqlStringDate.length());
            return Objects.equals(mysqlStringDate, substring);
        } else {
            String substring = mysqlStringDate.substring(0, esStringDate.length());
            return Objects.equals(esStringDate, substring);
        }
    }

    public static boolean equalsObjectFieldRowDataValue(Object mysql, Object es,
                                                        ESSyncConfig.ObjectField objectField,
                                                        Map<String, Object> mysqlRowData, Map<String, Object> esRowData,
                                                        boolean requireSequential) {
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
                return equalsRowDataValue(mysqlParse, es);
            }
            case LLM_VECTOR: {
//                boolean mysqlEmpty = mysql == null || "".equals(mysql);
//                boolean esEmpty = es == null || "".equals(es);
//                if (esEmpty) {
//                    return mysqlEmpty;
//                } else if (mysqlEmpty) {
//                    return false;
//                } else {
                String refTextFieldName = objectField.getParamLlmVector().getEtlEqualsFieldName();
                if (refTextFieldName == null || refTextFieldName.isEmpty()) {
                    throw new IllegalStateException(String.format("ParamLlmVector field '%s', fieldName must in select fields !",
                            objectField
                    ));
                }
                Object refEs = esRowData.get(refTextFieldName);
                Object refMysql = mysqlRowData.get(refTextFieldName);
                ESSyncConfig.ObjectField refObjectField = objectField.getEsMapping().getObjectField(null, refTextFieldName);
                if (refObjectField == null) {
                    return equalsRowDataValue(refMysql, refEs);
                } else {
                    return equalsObjectFieldRowDataValue(refMysql, refEs, refObjectField, mysqlRowData, esRowData, requireSequential);
                }
//                }
            }
            case ARRAY: {
                if (es == null && mysql == null) {
                    return true;
                }
                Collection<?> mysqlParse = (Collection) objectField.parse(mysql, objectField.getEsMapping(), mysqlRowData);
                if (es == null && mysqlParse == null) {
                    return true;
                }
                Collection<?> esParse;
                if (es instanceof Collection) {
                    esParse = (Collection) es;
                } else {
                    return false;
                }
                return equalsToStringList(mysqlParse, esParse, requireSequential);
            }
            case BOOLEAN: {
                try {
                    return Objects.equals(TypeUtil.castToBoolean(mysql), TypeUtil.castToBoolean(es));
                } catch (Exception e) {
                    return false;
                }
            }
            default: {
                return equalsRowDataValue(mysql, es);
            }
        }
    }

    private static boolean equalsToStringList(Collection<?> mysqlList, Collection<?> esList, boolean requireSequential) {
        if (mysqlList == null && esList == null) {
            return true;
        }
        if (mysqlList == null || esList == null) {
            return false;
        }
        if (mysqlList.size() != esList.size()) {
            return false;
        }
        if (requireSequential) {
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
        } else {
            Set<String> mysqlStringList = mysqlList.stream()
                    .map(e -> Objects.toString(e, null))
                    .collect(Collectors.toSet());
            for (Object es : esList) {
                String esString = Objects.toString(es, null);
                if (!mysqlStringList.contains(esString)) {
                    return false;
                }
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
                    ESSyncConfig.ObjectField childObjectField = objectField.getEsMapping().getObjectField(objectField.getFieldName(), field);
                    if (childObjectField != null) {
                        if (!equalsObjectFieldRowDataValue(mysqlValue, esValue, childObjectField, mysql, es, true)) {
                            return false;
                        }
                    } else if (!equalsRowDataValue(mysqlValue, esValue)) {
                        return false;
                    }
                }
                return true;
            } else if (esRowData instanceof Collection) {
                Collection<?> esList = (Collection<?>) esRowData;
                if (esList.size() != mysqlRowData.size()) {
                    return false;
                }
                boolean requireSequential = isRequireSequential(objectField);
                if (objectField.getType().isFlatSqlType()) {
                    List<Object> mysqlRowFlatData = flatValue0List(mysqlRowData);
                    return equalsToStringList(mysqlRowFlatData, esList, requireSequential);
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
                                if (!equalsObjectFieldRowDataValue(mysqlValue, esValue, fieldObjectField, mysql, es, requireSequential)) {
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

    /**
     * 是否需要保持顺序
     *
     * @param objectField 字段
     * @return true=需要保持顺序
     */
    private static boolean isRequireSequential(ESSyncConfig.ObjectField objectField) {
        return Optional.ofNullable(objectField.getParamSql())
                .map(ESSyncConfig.ObjectField.ParamSql::getSchemaItem)
                .map(SchemaItem::existAnyOrderColumn)
                .orElse(Boolean.FALSE);
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
                boolean requireSequential = isRequireSequential(objectField);
                if (!equalsObjectFieldRowDataValue(mysql, es, objectField, mysqlRowData, esRowData, requireSequential)) {
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
        } else if (mysql instanceof Date || es instanceof Date) {
            equals = equalsEsDate(mysql, es);
        } else if (es instanceof Collection) {
            // 这里应该进不来，进来就是bug（需要外层调用换成 equalsObjectFieldRowDataValue）
            String mysqlString = mysql.toString();
            for (Object esItem : (Collection) es) {
                if (esItem == null || !mysqlString.contains(esItem.toString())) {
                    return false;
                }
            }
            return true;
        } else if (es instanceof Map) {
            // 这里除了经纬度，其他的应该进不来，其他的进来就是bug（需要外层调用换成 equalsObjectFieldRowDataValue）
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
