package com.github.dts.util;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.*;

public class EsTypeUtil {

    private static final Logger log = LoggerFactory.getLogger(EsTypeUtil.class);
    private static final String[] ES_FORMAT_SUPPORT = {"yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd"};

    /**
     * 类型转换为Mapping中对应的类型
     *
     * @param mysqlValue      val
     * @param fieldName       fileName
     * @param parentFieldName parentFieldName
     * @param esTypes         esTypes
     * @return Mapping中对应的类型
     */
    public static Object mysql2EsType(ESSyncConfig.ESMapping mapping, Map<String, Object> mysqlRow, Object mysqlValue, String fieldName, ESFieldTypesCache esTypes, String parentFieldName) {
        // 如果是对象类型
        ESSyncConfig.ObjectField objectField = mapping.getObjectField(parentFieldName, fieldName);
        if (objectField != null) {
            return objectField.parse(mysqlValue, mapping, mysqlRow);
        }
        if (mysqlValue == null) {
            return null;
        }
        if (esTypes == null) {
            return mysqlValue;
        }
        Object esType;
        if (parentFieldName != null) {
            Map<String, Object> properties = esTypes.getProperties(parentFieldName, "properties");
            if (properties == null) {
                return mysqlValue;
            }
            Object typeMap = properties.get(fieldName);
            if (typeMap instanceof Map) {
                esType = ((Map<?, ?>) typeMap).get("type");
            } else {
                esType = typeMap;
            }
        } else {
            esType = esTypes.get(fieldName);
        }

        Object res = mysqlValue;
        if ("keyword".equals(esType) || "text".equals(esType)) {
            res = mysqlValue.toString();
        } else if ("integer".equals(esType)) {
            if (mysqlValue instanceof Number) {
                res = ((Number) mysqlValue).intValue();
            } else {
                res = Integer.parseInt(mysqlValue.toString());
            }
        } else if ("long".equals(esType)) {
            if (mysqlValue instanceof Number) {
                res = ((Number) mysqlValue).longValue();
            } else {
                res = Long.parseLong(mysqlValue.toString());
            }
        } else if ("short".equals(esType)) {
            if (mysqlValue instanceof Number) {
                res = ((Number) mysqlValue).shortValue();
            } else {
                res = Short.parseShort(mysqlValue.toString());
            }
        } else if ("byte".equals(esType)) {
            if (mysqlValue instanceof Number) {
                res = ((Number) mysqlValue).byteValue();
            } else {
                res = Byte.parseByte(mysqlValue.toString());
            }
        } else if ("double".equals(esType)) {
            if (mysqlValue instanceof Number) {
                res = ((Number) mysqlValue).doubleValue();
            } else {
                res = Double.parseDouble(mysqlValue.toString());
            }
        } else if ("float".equals(esType) || "half_float".equals(esType) || "scaled_float".equals(esType)) {
            if (mysqlValue instanceof Number) {
                res = ((Number) mysqlValue).floatValue();
            } else {
                res = Float.parseFloat(mysqlValue.toString());
            }
        } else if ("boolean".equals(esType)) {
            if (mysqlValue instanceof Boolean) {
                res = mysqlValue;
            } else if (mysqlValue instanceof Number) {
                int v = ((Number) mysqlValue).intValue();
                res = v != 0;
            } else {
                res = Boolean.parseBoolean(mysqlValue.toString());
            }
        } else if ("date".equals(esType)) {
            if (mysqlValue instanceof java.sql.Time) {
                DateTime dateTime = new DateTime(((java.sql.Time) mysqlValue).getTime());
                res = parseDate(fieldName, parentFieldName, dateTime, esTypes);
            } else if (mysqlValue instanceof java.sql.Timestamp) {
                DateTime dateTime = new DateTime(((java.sql.Timestamp) mysqlValue).getTime());
                res = parseDate(fieldName, parentFieldName, dateTime, esTypes);
            } else if (mysqlValue instanceof java.sql.Date || mysqlValue instanceof Date) {
                DateTime dateTime;
                if (mysqlValue instanceof java.sql.Date) {
                    dateTime = new DateTime(((java.sql.Date) mysqlValue).getTime());
                } else {
                    dateTime = new DateTime(((Date) mysqlValue).getTime());
                }
                res = parseDate(fieldName, parentFieldName, dateTime, esTypes);
            } else if (mysqlValue instanceof Long) {
                DateTime dateTime = new DateTime(((Long) mysqlValue).longValue());
                res = parseDate(fieldName, parentFieldName, dateTime, esTypes);
            } else if (mysqlValue instanceof String) {
                String v = ((String) mysqlValue).trim();
                if (v.length() > 18 && v.charAt(4) == '-' && v.charAt(7) == '-' && v.charAt(10) == ' '
                        && v.charAt(13) == ':' && v.charAt(16) == ':') {
                    String dt = v.substring(0, 10) + "T" + v.substring(11);
                    Date date = Util.parseDate(dt);
                    if (date != null) {
                        DateTime dateTime = new DateTime(date);
                        res = parseDate(fieldName, parentFieldName, dateTime, esTypes);
                    }
                } else if (v.length() == 10 && v.charAt(4) == '-' && v.charAt(7) == '-') {
                    Date date = Util.parseDate(v);
                    if (date != null) {
                        DateTime dateTime = new DateTime(date);
                        res = parseDate(fieldName, parentFieldName, dateTime, esTypes);
                    }
                } else if (v.length() == 7 && v.charAt(4) == '-') {
                    Date date = Util.parseDate(v);
                    if (date != null) {
                        DateTime dateTime = new DateTime(date);
                        res = parseDate(fieldName, parentFieldName, dateTime, esTypes);
                    }
                } else if (v.length() == 4) {
                    Date date = Util.parseDate(v);
                    if (date != null) {
                        DateTime dateTime = new DateTime(date);
                        res = parseDate(fieldName, parentFieldName, dateTime, esTypes);
                    }
                }
            }
        } else if ("binary".equals(esType)) {
            if (mysqlValue instanceof byte[]) {
                res = new String(Base64.getEncoder().encode((byte[]) mysqlValue), Charset.forName("UTF-8"));
            } else if (mysqlValue instanceof Blob) {
                byte[] b = blobToBytes((Blob) mysqlValue);
                res = new String(Base64.getEncoder().encode(b), Charset.forName("UTF-8"));
            } else if (mysqlValue instanceof String) {
                // 对应canal中的单字节编码
                byte[] b = ((String) mysqlValue).getBytes(StandardCharsets.ISO_8859_1);
                res = new String(Base64.getEncoder().encode(b), Charset.forName("UTF-8"));
            }
        } else if ("geo_point".equals(esType)) {
            if (!(mysqlValue instanceof String)) {
//                log.error("es type is geo_point, but source type is not String");
                return mysqlValue;
            }

            if (!((String) mysqlValue).contains(",")) {
                log.error("es type is geo_point, source value not contains ',' separator {} {} = {}", parentFieldName, fieldName, mysqlValue);
                return mysqlValue;
            }
            return ESSyncUtil.parseGeoPointToMap((String) mysqlValue);
        } else if ("array".equals(esType)) {
            if ("".equals(mysqlValue.toString().trim())) {
                res = new ArrayList<>();
            } else {
                String value = mysqlValue.toString();
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
            if ("".equals(mysqlValue.toString().trim())) {
                res = new HashMap<>();
            } else {
                res = JsonUtil.toMap(mysqlValue.toString(), true);
            }
        } else {
            if (mysqlValue instanceof Date) {
                DateTime dateTime = new DateTime(((Date) mysqlValue).getTime());
                res = dateTime.toString("yyyy-MM-dd HH:mm:ss.SSS");
            } else if (mysqlValue instanceof Map) {
            } else if (mysqlValue instanceof Collection) {
            } else if (mysqlValue.getClass().isArray()) {
            } else if (mysqlValue instanceof Number) {
            } else {
                // 其他类全以字符串处理
                res = mysqlValue.toString();
            }
        }

        return res;
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
            log.error("blobToBytes error {}, ", e.toString(), e);
            return null;
        }
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

}
