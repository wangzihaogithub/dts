package com.github.dts.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.*;

public class EsTypeUtil {
    private static final Logger log = LoggerFactory.getLogger(EsTypeUtil.class);
    private static String[] ES_DATE_FORMAT_SUPPORT = {"yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd"};
    private static final JsonUtil.ObjectReader OBJECT_READER = JsonUtil.objectReader();

    public static void setEsDateFormatSupport(String[] esDateFormatSupport) {
        ES_DATE_FORMAT_SUPPORT = esDateFormatSupport;
    }

    public static String[] getEsDateFormatSupport() {
        return ES_DATE_FORMAT_SUPPORT;
    }

    public static Map<String, Object> mysql2EsType(ESSyncConfig.ESMapping mapping,
                                                   Map<String, Object> mysqlData,
                                                   ESFieldTypesCache esFieldType) {
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
            ESFieldTypesCache esType = esFieldType == null ? null : esFieldType.getField(k);
            Object res = mysql2EsType(mapping, mysqlData, val, k, esType, null);
            result.put(k, res);
        }
        return Util.trimToSize(result, LinkedHashMap::new);
    }

    private static Map<String, Object> mysql2EsTypeObjectSql(ESSyncConfig.ESMapping mapping,
                                                             Map<String, Object> mysqlValueGetter,
                                                             String fieldName,
                                                             ESFieldTypesCache esTypes,
                                                             boolean cast) {
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : mysqlValueGetter.entrySet()) {
            Object value = entry.getValue();
            String key = entry.getKey();
            Object valueCast;
            if (cast) {
                valueCast = mysql2EsType(mapping, mysqlValueGetter, value, key, esTypes == null ? null : esTypes.getField(key), fieldName);
            } else {
                valueCast = value;
            }
            copy.put(key, valueCast);
        }
        return copy;
    }

    /**
     * 类型转换为Mapping中对应的类型
     *
     * @param mysqlValue      val
     * @param fieldName       fileName
     * @param parentFieldName parentFieldName
     * @param esTypes         esTypes
     * @param mapping         mapping
     * @param mysqlRow        mysqlRow
     * @return Mapping中对应的类型
     */
    private static Object mysql2EsType(ESSyncConfig.ESMapping mapping,
                                       Map<String, Object> mysqlRow,
                                       Object mysqlValue,
                                       String fieldName,
                                       ESFieldTypesCache esTypes,
                                       String parentFieldName) {
        // 如果是对象类型
        ESSyncConfig.ObjectField objectField = mapping.getObjectField(parentFieldName, fieldName);
        if (objectField != null) {
            ESSyncConfig.ObjectField.Type type = objectField.getType();
            switch (type) {
                case OBJECT_SQL: {//mysql2EsType
                    if (mysqlValue instanceof Map) {
                        return mysql2EsTypeObjectSql(mapping, (Map<String, Object>) mysqlValue, fieldName, esTypes, parentFieldName == null);
                    }
                    return mysqlValue;
                }
                case ARRAY_SQL: {//mysql2EsType
                    if (mysqlValue instanceof Collection) {
                        Collection<Map<String, Object>> mysqlValueGetter = (List<Map<String, Object>>) mysqlValue;
                        List<Map<String, Object>> list = new ArrayList<>(mysqlValueGetter.size());
                        for (Map<String, Object> row : mysqlValueGetter) {
                            Map<String, Object> map = mysql2EsTypeObjectSql(mapping, row, fieldName, esTypes, parentFieldName == null);
                            list.add(map);
                        }
                        return list;
                    }
                    return mysqlValue;
                }
                case ARRAY_FLAT_SQL: {//mysql2EsType
                    if (mysqlValue instanceof Collection) {
                        List<Object> mysqlValueGetter = (List<Object>) mysqlValue;
                        List<Object> result = new ArrayList<>(mysqlValueGetter.size());
                        for (Object e : mysqlValueGetter) {
                            result.add(parseEsType(e, esTypes));
                        }
                        return result;
                    }
                    return mysqlValue;
                }
                case OBJECT_FLAT_SQL: {//mysql2EsType
                    return parseEsType(mysqlValue, esTypes);
                }
                default: {
                    return objectField.parse(mysqlValue, mapping, mysqlRow);
                }
            }
        } else {
            return parseEsType(mysqlValue, esTypes);
        }
    }

    private static Object parseEsType(Object mysqlValue, ESFieldTypesCache esTypeVO) {
        if (mysqlValue == null) {
            return null;
        }
        // 如果es mapping里没有这个属性
        if (esTypeVO == null) {
            if (mysqlValue instanceof Date) {
                return DateUtil.dateFormat((Date) mysqlValue, ES_DATE_FORMAT_SUPPORT[0]);
            } else if (mysqlValue instanceof Number) {
                return mysqlValue;
            } else if (mysqlValue instanceof Boolean) {
                return mysqlValue;
            } else {
                return mysqlValue.toString();
            }
        }
        Object res = mysqlValue;
        Object esType = esTypeVO.getType();
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
            if (mysqlValue instanceof Date) {
                res = parseEsDate((Date) mysqlValue, esTypeVO);
            } else if (mysqlValue instanceof Long) {
                res = parseEsDate(new Date((Long) mysqlValue), esTypeVO);
            } else if (mysqlValue instanceof String) {
                String v = ((String) mysqlValue).trim();
                if (v.length() > 18 && v.charAt(4) == '-' && v.charAt(7) == '-' && v.charAt(10) == ' '
                        && v.charAt(13) == ':' && v.charAt(16) == ':') {
                    String dt = v.substring(0, 10) + "T" + v.substring(11);
                    Date date = DateUtil.parseDate(dt);
                    if (date != null) {
                        res = parseEsDate(date, esTypeVO);
                    }
                } else if (v.length() == 10 && v.charAt(4) == '-' && v.charAt(7) == '-') {
                    Date date = DateUtil.parseDate(v);
                    if (date != null) {
                        res = parseEsDate(date, esTypeVO);
                    }
                } else if (v.length() == 7 && v.charAt(4) == '-') {
                    Date date = DateUtil.parseDate(v);
                    if (date != null) {
                        res = parseEsDate(date, esTypeVO);
                    }
                } else if (v.length() == 4) {
                    Date date = DateUtil.parseDate(v);
                    if (date != null) {
                        res = parseEsDate(date, esTypeVO);
                    }
                }
            }
        } else if ("binary".equals(esType)) {
            if (mysqlValue instanceof byte[]) {
                res = new String(Base64.getEncoder().encode((byte[]) mysqlValue), StandardCharsets.UTF_8);
            } else if (mysqlValue instanceof Blob) {
                byte[] b = blobToBytes((Blob) mysqlValue);
                res = new String(Base64.getEncoder().encode(b), StandardCharsets.UTF_8);
            } else if (mysqlValue instanceof String) {
                // 对应canal中的单字节编码
                byte[] b = ((String) mysqlValue).getBytes(StandardCharsets.ISO_8859_1);
                res = new String(Base64.getEncoder().encode(b), StandardCharsets.UTF_8);
            }
        } else if ("geo_point".equals(esType)) {
            if (!(mysqlValue instanceof String)) {
//                log.error("es type is geo_point, but source type is not String");
                return mysqlValue;
            }

            if (!((String) mysqlValue).contains(",")) {
                log.error("es type is geo_point, source value not contains ',' separator {} = {}", esTypeVO, mysqlValue);
                return mysqlValue;
            }
            return ESSyncUtil.parseGeoPointToMap((String) mysqlValue);
        } else if ("array".equals(esType)) {
            if (Util.isBlank(mysqlValue.toString())) {
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
            if (Util.isBlank(mysqlValue.toString())) {
                res = new HashMap<>();
            } else {
                try {
                    res = OBJECT_READER.readValue(mysqlValue.toString(), Map.class);
                } catch (IOException e) {
                    Util.sneakyThrows(e);
                }
            }
        } else {
            if (mysqlValue instanceof Date) {
                res = DateUtil.dateFormat((Date) mysqlValue, "yyyy-MM-dd HH:mm:ss.SSS");
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

    private static Object parseEsDate(Date dateTime, ESFieldTypesCache esFieldType) {
        Set<String> esFormatSet = esFieldType.getFormatSet();
        if (esFormatSet != null) {
            if (esFormatSet.size() == 1) {
                return DateUtil.dateFormat(dateTime, esFormatSet.iterator().next());
            }
            for (String format : ES_DATE_FORMAT_SUPPORT) {
                if (esFormatSet.contains(format)) {
                    return DateUtil.dateFormat(dateTime, format);
                }
            }
            if (esFormatSet.contains("epoch_millis")) {
                return dateTime.getTime();
            }
        }
        ZonedDateTime zonedDateTime = dateTime.toInstant().atZone(DateUtil.zoneId);
        if (zonedDateTime.getHour() == 0 && zonedDateTime.getMinute() == 0 && zonedDateTime.getSecond() == 0
                && zonedDateTime.getNano() == 0) {
            return DateUtil.dateFormat(dateTime, "yyyy-MM-dd");
        } else {
            if (zonedDateTime.getNano() != 0) {
                return DateUtil.dateFormat(dateTime, "yyyy-MM-dd HH:mm:ss.SSS");
            } else {
                return DateUtil.dateFormat(dateTime, "yyyy-MM-dd HH:mm:ss");
            }
        }
    }

}
