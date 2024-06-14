package com.github.dts.util;

import com.github.dts.impl.elasticsearch7x.ES7xAdapter;
import com.github.dts.impl.elasticsearch7x.NestedFieldWriter;
import com.github.dts.util.ESSyncConfig.ESMapping;
import com.github.dts.util.SchemaItem.ColumnItem;
import com.github.dts.util.SchemaItem.TableItem;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.*;
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

    /**
     * 多个集合合并为一个集合
     *
     * @param lists 多个集合
     * @param <T>
     * @return 一个集合
     */
    public static <T> List<T> mergeList(List<T>... lists) {
        List<T> list = new ArrayList<>();
        for (List<T> l : lists) {
            list.addAll(l);
        }
        return list;
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
        if (CanalConfig.DatasourceConfig.DATA_SOURCES.containsValue(jdbcTemplate.getDataSource())) {
            return jdbcTemplate;
        }
        synchronized (JDBC_TEMPLATE_MAP) {
            JDBC_TEMPLATE_MAP.clear();
            jdbcTemplate = new JdbcTemplate(CanalConfig.DatasourceConfig.getDataSource(srcDataSourcesKey));
            JDBC_TEMPLATE_MAP.put(srcDataSourcesKey, jdbcTemplate);
        }
        return jdbcTemplate;
    }

    /**
     * 获取默认的数据源
     *
     * @return 数据源
     */
    public static JdbcTemplate getJdbcTemplateByDefaultDS() {
        return ESSyncUtil.getJdbcTemplateByKey("defaultDS");
    }

    /**
     * 根据ES类型 获取所有索引
     *
     * @return 所有索引
     */
    public static List<ESSyncConfig.ESMapping> getESMappingByESType(String destination, Collection<ESSyncConfig> esSyncConfigs) {
        return esSyncConfigs.stream()
                .filter(e -> Objects.equals(e.getDestination(), destination))
                .map(ESSyncConfig::getEsMapping)
                .collect(Collectors.toList());
    }

    /**
     * 更新所有此ES类型的数据 (条件=主键)
     *
     * @param pkValue     主键
     * @param esFieldData 需要修改的数据
     */
    public static void updateESByPrimaryKey(Object pkValue,
                                            Map<String, Object> esFieldData,
                                            ESSyncConfig.ESMapping esMapping, ESTemplate esTemplate, ESTemplate.BulkRequestList bulkRequestList) {
        if (log.isDebugEnabled()) {
            log.debug("updateESByPrimaryKey -> pkValue={}", pkValue);
        }

        if (esFieldData == null || esFieldData.isEmpty()) {
            return;
        }
        //更新ES文档 (执行完会统一提交, 这里不用commit)
        esTemplate.update(esMapping, pkValue, esFieldData, bulkRequestList);
    }

    /**
     * 更新所有此ES类型的数据
     *
     * @param updateWhere 更新条件
     * @param esFieldData 需要修改的数据
     */
    public static void updateESByQuery(Map<String, Object> updateWhere,
                                       Map<String, Object> esFieldData, Collection<ESSyncConfig.ESMapping> esMappingByESType,
                                       ESTemplate esTemplate, ESTemplate.BulkRequestList bulkRequestList) {
        if (log.isDebugEnabled()) {
            log.debug("updateESByQuery -> updateWhere={}, esFieldData={} ", updateWhere, esFieldData);
        }

        if (updateWhere == null || updateWhere.isEmpty()) {
            return;
        }
        if (esFieldData == null || esFieldData.isEmpty()) {
            return;
        }
        //更新ES文档 (执行完会统一提交, 这里不用commit)
        for (ESSyncConfig.ESMapping esMapping : esMappingByESType) {
            esTemplate.updateByQuery(esMapping, updateWhere, esFieldData, bulkRequestList);
        }
    }

    public static void deleteEsByPrimaryKey(Object pkValue,
                                            Map<String, Object> esFieldData,
                                            Collection<ESSyncConfig.ESMapping> esMappingByESType, ESTemplate esTemplate,
                                            ESTemplate.BulkRequestList bulkRequestList) {

        if (pkValue == null) {
            return;
        }
        //更新ES文档 (执行完会统一提交, 这里不用commit)
        for (ESSyncConfig.ESMapping esMapping : esMappingByESType) {
            esTemplate.delete(esMapping, pkValue, esFieldData, bulkRequestList);
        }
    }

    /**
     * 脚本更新
     *
     * @param idOrCode 脚本
     * @param pkValue  主键
     */
    public static void updateByScript(String idOrCode, int scriptTypeId, Object pkValue, String lang,
                                      Map<String, Object> params, Collection<ESSyncConfig.ESMapping> esMappingByESType,
                                      ESTemplate esTemplate, ESTemplate.BulkRequestList bulkRequestList) {
        if (log.isDebugEnabled()) {
            log.debug("updateByScript -> pkValue={}, script={} ", pkValue, idOrCode);
        }

        for (ESSyncConfig.ESMapping esMapping : esMappingByESType) {
            esTemplate.updateByScript(esMapping, idOrCode, false, pkValue, scriptTypeId, lang, params, bulkRequestList);
        }
    }

    /**
     * 转换为ES对象
     * <p>
     * |ARRAY   |OBJECT     |ARRAY_SQL      |OBJECT_SQL      |
     * |数组     | 对象      |数组sql查询多条  |对象sql查询单条   |
     * <p>
     * 该方法只实现 ARRAY与OBJECT
     * ARRAY_SQL与OBJECT_SQL的实现 -> {@link NestedFieldWriter}
     *
     * @param val
     * @param mapping
     * @param fieldName
     * @param data
     * @return
     * @see ESSyncServiceListener#onSyncAfter(List, ES7xAdapter, ESTemplate.BulkRequestList)
     */
    public static Object convertToEsObj(Object val, ESMapping mapping, String fieldName, Map<String, Object> data) {
        if (val == null) {
            return null;
        }

        ESSyncConfig.ObjectField objectField = mapping.getObjFields().get(fieldName);
        switch (objectField.getType()) {
            case ARRAY: {
                String varStr = val.toString();
                if (StringUtils.isEmpty(varStr)) {
                    return null;
                }
                String[] values = varStr.split(objectField.getSplit());
                return Arrays.asList(values);
            }
            case OBJECT: {
                return JsonUtil.toMap(val.toString(), true);
            }
            case BOOLEAN: {
                return castToBoolean(val);
            }
            case STATIC_METHOD: {
                return objectField.staticMethodAccessor().apply(new ESStaticMethodParam(val, mapping, fieldName, data));
            }
            case ARRAY_SQL:
            case OBJECT_SQL:
            default: {
                return val;
            }
        }
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
     */
    public static Object typeConvert(Object val, String esType) {
        if (val == null) {
            return null;
        }
        if (esType == null) {
            return val;
        }
        Object res = null;
        if ("integer".equals(esType)) {
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
                if (dateTime.getMillisOfSecond() != 0) {
                    res = dateTime.toString("HH:mm:ss.SSS");
                } else {
                    res = dateTime.toString("HH:mm:ss");
                }
            } else if (val instanceof java.sql.Timestamp) {
                DateTime dateTime = new DateTime(((java.sql.Timestamp) val).getTime());
                if (dateTime.getMillisOfSecond() != 0) {
                    res = dateTime.toString("yyyy-MM-dd HH:mm:ss.SSS");
                } else {
                    res = dateTime.toString("yyyy-MM-dd HH:mm:ss");
                }
            } else if (val instanceof java.sql.Date || val instanceof Date) {
                DateTime dateTime;
                if (val instanceof java.sql.Date) {
                    dateTime = new DateTime(((java.sql.Date) val).getTime());
                } else {
                    dateTime = new DateTime(((Date) val).getTime());
                }
                if (dateTime.getHourOfDay() == 0 && dateTime.getMinuteOfHour() == 0 && dateTime.getSecondOfMinute() == 0
                        && dateTime.getMillisOfSecond() == 0) {
                    res = dateTime.toString("yyyy-MM-dd");
                } else {
                    if (dateTime.getMillisOfSecond() != 0) {
                        res = dateTime.toString("yyyy-MM-dd HH:mm:ss.SSS");
                    } else {
                        res = dateTime.toString("yyyy-MM-dd HH:mm:ss");
                    }
                }
            } else if (val instanceof Long) {
                DateTime dateTime = new DateTime(((Long) val).longValue());
                if (dateTime.getHourOfDay() == 0 && dateTime.getMinuteOfHour() == 0 && dateTime.getSecondOfMinute() == 0
                        && dateTime.getMillisOfSecond() == 0) {
                    res = dateTime.toString("yyyy-MM-dd");
                } else if (dateTime.getMillisOfSecond() != 0) {
                    res = dateTime.toString("yyyy-MM-dd HH:mm:ss.SSS");
                } else {
                    res = dateTime.toString("yyyy-MM-dd HH:mm:ss");
                }
            } else if (val instanceof String) {
                String v = ((String) val).trim();
                if (v.length() > 18 && v.charAt(4) == '-' && v.charAt(7) == '-' && v.charAt(10) == ' '
                        && v.charAt(13) == ':' && v.charAt(16) == ':') {
                    String dt = v.substring(0, 10) + "T" + v.substring(11);
                    Date date = Util.parseDate(dt);
                    if (date != null) {
                        DateTime dateTime = new DateTime(date);
                        if (dateTime.getMillisOfSecond() != 0) {
                            res = dateTime.toString("yyyy-MM-dd HH:mm:ss.SSS");
                        } else {
                            res = dateTime.toString("yyyy-MM-dd HH:mm:ss");
                        }
                    }
                } else if (v.length() == 10 && v.charAt(4) == '-' && v.charAt(7) == '-') {
                    Date date = Util.parseDate(v);
                    if (date != null) {
                        DateTime dateTime = new DateTime(date);
                        res = dateTime.toString("yyyy-MM-dd");
                    }
                } else if (v.length() == 7 && v.charAt(4) == '-') {
                    Date date = Util.parseDate(v);
                    if (date != null) {
                        DateTime dateTime = new DateTime(date);
                        res = dateTime.toString("yyyy-MM");
                    }
                } else if (v.length() == 4) {
                    Date date = Util.parseDate(v);
                    if (date != null) {
                        DateTime dateTime = new DateTime(date);
                        res = dateTime.toString("yyyy");
                    }
                }
            }
        } else if ("binary".equals(esType)) {
            if (val instanceof byte[]) {
                Base64 base64 = new Base64();
                res = base64.encodeAsString((byte[]) val);
            } else if (val instanceof Blob) {
                byte[] b = blobToBytes((Blob) val);
                Base64 base64 = new Base64();
                res = base64.encodeAsString(b);
            } else if (val instanceof String) {
                // 对应canal中的单字节编码
                byte[] b = ((String) val).getBytes(StandardCharsets.ISO_8859_1);
                Base64 base64 = new Base64();
                res = base64.encodeAsString(b);
            }
        } else if ("geo_point".equals(esType)) {
            if (!(val instanceof String)) {
                log.error("es type is geo_point, but source type is not String");
                return val;
            }

            if (!((String) val).contains(",")) {
                log.error("es type is geo_point, source value not contains ',' separator");
                return val;
            }

            String[] point = ((String) val).split(",");
            Map<String, Double> location = new HashMap<>();
            location.put("lat", Double.valueOf(point[0].trim()));
            location.put("lon", Double.valueOf(point[1].trim()));
            return location;
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

    /**
     * Blob转byte[]
     */
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

    /**
     * 拼接主键条件
     *
     * @param mapping
     * @param data
     * @return
     */
    public static String pkConditionSql(ESMapping mapping, Map<String, Object> data) {
        Set<ColumnItem> idColumns = new LinkedHashSet<>();
        SchemaItem schemaItem = mapping.getSchemaItem();

        TableItem mainTable = schemaItem.getMainTable();

        for (ColumnItem idColumnItem : schemaItem.getIdFieldItem(mapping).getColumnItems()) {
            if ((mainTable.getAlias() == null && idColumnItem.getOwner() == null)
                    || (mainTable.getAlias() != null && mainTable.getAlias().equals(idColumnItem.getOwner()))) {
                idColumns.add(idColumnItem);
            }
        }

        if (idColumns.isEmpty()) {
            throw new RuntimeException("Not found primary key field in main table");
        }

        // 拼接condition
        StringBuilder condition = new StringBuilder(" ");
        for (ColumnItem idColumn : idColumns) {
            Object idVal = data.get(idColumn.getColumnName());
            if (mainTable.getAlias() != null) {
                condition.append(mainTable.getAlias()).append(".");
            }
            condition.append(idColumn.getColumnName()).append("=");
            if (idVal instanceof String) {
                condition.append("'").append(idVal).append("' AND ");
            } else {
                condition.append(idVal).append(" AND ");
            }
        }

        if (condition.toString().endsWith("AND ")) {
            int len2 = condition.length();
            condition.delete(len2 - 4, len2);
        }
        return condition.toString();
    }

    public static String appendCondition(String sql, String condition) {
        return sql + " WHERE " + condition + " ";
    }

    public static void appendCondition(StringBuilder sql, Object value, String owner, String columnName) {
        if (value instanceof String) {
            sql.append(owner).append(".").append(columnName).append("='").append(value).append("'  AND ");
        } else {
            sql.append(owner).append(".").append(columnName).append("=").append(value).append("  AND ");
        }
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

    public static boolean isNotEmpty(Object object) {
        return !isEmpty(object);
    }

    public static Map<String, Object> convertType(Map<String, Object> esFieldData) {
        if (esFieldData == null || esFieldData.isEmpty()) {
            return esFieldData;
        }
        Map<String, Object> result = new HashMap<>();
        esFieldData.forEach((k, val) -> {
            if (val != null) {
                Object res = val;
                if (val instanceof java.sql.Timestamp) {
                    DateTime dateTime = new DateTime(((java.sql.Timestamp) val).getTime());
                    if (dateTime.getMillisOfSecond() != 0) {
                        res = dateTime.toString("yyyy-MM-dd HH:mm:ss.SSS");
                    } else {
                        res = dateTime.toString("yyyy-MM-dd HH:mm:ss");
                    }
                } else if (val instanceof java.sql.Date || val instanceof Date) {
                    DateTime dateTime;
                    if (val instanceof java.sql.Date) {
                        dateTime = new DateTime(((java.sql.Date) val).getTime());
                    } else {
                        dateTime = new DateTime(((Date) val).getTime());
                    }
                    if (dateTime.getHourOfDay() == 0 && dateTime.getMinuteOfHour() == 0 && dateTime.getSecondOfMinute() == 0
                            && dateTime.getMillisOfSecond() == 0) {
                        res = dateTime.toString("yyyy-MM-dd");
                    } else {
                        if (dateTime.getMillisOfSecond() != 0) {
                            res = dateTime.toString("yyyy-MM-dd HH:mm:ss.SSS");
                        } else {
                            res = dateTime.toString("yyyy-MM-dd HH:mm:ss");
                        }
                    }
                } else if (val instanceof Map) {
                    res = convertType((Map<String, Object>) val);
                } else if (val instanceof ArrayList) {
                    List list = (List) val;
                    List reslist = new ArrayList();
                    for (Object temp : list) {
                        if (temp instanceof Map) {
                            reslist.add(convertType((Map<String, Object>) temp));
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
        });
        return result;
    }
}
