/*
 * Copyright 1999-2017 Alibaba Group.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.dts.util;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author wenshao[szujobs@hotmail.com]
 */
public class TypeUtil {

    public static boolean isBasicType(Object value) {
        if (value == null) {
            return true;
        }
        Class<?> type = value.getClass();
        if (type.isPrimitive()
                || value instanceof Number
                || value instanceof CharSequence
                || value instanceof Date
                || value instanceof TemporalAccessor
                || value instanceof Enum) {
            return true;
        } else {
            Package aPackage = type.getPackage();
            if (aPackage == null) {
                return true;
            }
            return aPackage.getName().startsWith("java.");
        }
    }

    public static <T> T castBean(Object obj, String typeString) throws ClassNotFoundException {
        if (obj == null || typeString == null) {
            return (T) obj;
        }
        Class<T> type = (Class<T>) Class.forName(typeString);
        return TypeUtil.cast(obj, type);
    }

    public static <SOURCE extends Collection<?>, T> List<T> castBasic(SOURCE source, Class<T> type) {
        return source.stream()
                .map(e -> TypeUtil.cast(e, type))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public static String castToString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).stripTrailingZeros().toPlainString();
        }
        return value.toString();
    }

    public static Byte castToByte(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }

        if (value instanceof String) {
            String strVal = (String) value;
            if (strVal.length() == 0 //
                    || "null".equals(strVal) //
                    || "NULL".equals(strVal)) {
                return null;
            }

            return Byte.parseByte(strVal);
        }

        throw new RuntimeException("can not cast to byte, value : " + value);
    }

    public static Character castToChar(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Character) {
            return (Character) value;
        }

        if (value instanceof String) {
            String strVal = (String) value;

            if (strVal.length() == 0) {
                return null;
            }

            if (strVal.length() != 1) {
                throw new IllegalArgumentException("can not cast to char, value : " + value);
            }

            return strVal.charAt(0);
        }

        throw new IllegalArgumentException("can not cast to char, value : " + value);
    }

    public static Short castToShort(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }

        if (value instanceof String) {
            String strVal = (String) value;

            if (strVal.length() == 0 //
                    || "null".equals(strVal) //
                    || "NULL".equals(strVal)) {
                return null;
            }

            return Short.parseShort(strVal);
        }

        throw new IllegalArgumentException("can not cast to short, value : " + value);
    }

    public static BigDecimal castToBigDecimal(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }

        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        }

        String strVal = value.toString();
        if (strVal.length() == 0) {
            return null;
        }
        return new BigDecimal(strVal);
    }

    public static BigInteger castToBigInteger(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof BigInteger) {
            return (BigInteger) value;
        }

        if (value instanceof Float || value instanceof Double) {
            return BigInteger.valueOf(((Number) value).longValue());
        }

        String strVal = value.toString();
        if (strVal.length() == 0 //
                || "null".equals(strVal) //
                || "NULL".equals(strVal)) {
            return null;
        }

        return new BigInteger(strVal);
    }

    public static Float castToFloat(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }

        if (value instanceof String) {
            String strVal = value.toString();
            if (strVal.length() == 0 //
                    || "null".equals(strVal) //
                    || "NULL".equals(strVal)) {
                return null;
            }

            return Float.parseFloat(strVal);
        }

        throw new IllegalArgumentException("can not cast to float, value : " + value);
    }

    public static Double castToDouble(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }

        if (value instanceof String) {
            String strVal = value.toString();
            if (strVal.length() == 0 //
                    || "null".equals(strVal) //
                    || "NULL".equals(strVal)) {
                return null;
            }

            return Double.parseDouble(strVal);
        }

        throw new IllegalArgumentException("can not cast to double, value : " + value);
    }

    public static Date castToDate(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Date) { // 使用频率最高的，应优先处理
            return (Date) value;
        }

        if (value instanceof Calendar) {
            return ((Calendar) value).getTime();
        }

        if (value instanceof Number) {
            long longValue = ((Number) value).longValue();
            return new Date(longValue);
        }

        if (value instanceof String) {
            String strVal = (String) value;
            if (strVal.length() == 0 //
                    || "null".equals(strVal) //
                    || "NULL".equals(strVal)) {
                return null;
            }
            Date date = DateUtil.parseDate(strVal);
            if (date != null) {
                return date;
            }
        } else if (value instanceof LocalDateTime) {
            return new Date(((LocalDateTime) value).atZone(DateUtil.zoneId).toEpochSecond());
        } else if (value instanceof LocalDate) {
            return new Date(((LocalDate) value).atStartOfDay(DateUtil.zoneId).toEpochSecond());
        }
        throw new IllegalArgumentException("can not cast to Date, value : " + value);
    }

    /**
     * <p>Checks if the String contains only unicode digits.
     * A decimal point is not a unicode digit and returns false.</p>
     *
     * <p><code>null</code> will return <code>false</code>.
     * An empty String ("") will return <code>false</code>.</p>
     *
     * <pre>
     * StringUtils.isNumeric(null)   = false
     * StringUtils.isNumeric("")     = false
     * StringUtils.isNumeric("  ")   = false
     * StringUtils.isNumeric("123")  = true
     * StringUtils.isNumeric("12 3") = false
     * StringUtils.isNumeric("ab2c") = false
     * StringUtils.isNumeric("12-3") = false
     * StringUtils.isNumeric("12.3") = false
     * </pre>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if only contains digits, and is non-null
     */
    public static boolean isNumeric(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static Integer[] parseIntegerNumbers(String str) {
        if (str == null) {
            return new Integer[0];
        }
        List<Integer> result = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c >= '0' && c <= '9') {
                builder.append(c);
            } else if (builder.length() > 0) {
                result.add(Integer.valueOf(builder.toString()));
                builder.setLength(0);
            }
        }
        if (builder.length() > 0) {
            result.add(Integer.valueOf(builder.toString()));
        }
        return result.toArray(new Integer[0]);
    }

    public static java.sql.Date castToSqlDate(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof java.sql.Date) {
            return (java.sql.Date) value;
        }
        if (value instanceof Date) {
            return new java.sql.Date(((Date) value).getTime());
        }
        if (value instanceof Calendar) {
            return new java.sql.Date(((Calendar) value).getTimeInMillis());
        }

        if (value instanceof Number) {
            long longValue = ((Number) value).longValue();
            return new java.sql.Date(longValue);
        }

        if (value instanceof String) {
            String strVal = (String) value;
            if (strVal.length() == 0 //
                    || "null".equals(strVal) //
                    || "NULL".equals(strVal)) {
                return null;
            }
            Date date = DateUtil.parseDate(strVal);
            if (date != null) {
                return new java.sql.Date(date.getTime());
            }
        } else if (value instanceof LocalDateTime) {
            return new java.sql.Date(((LocalDateTime) value).atZone(DateUtil.zoneId).toEpochSecond());
        } else if (value instanceof LocalDate) {
            return new java.sql.Date(((LocalDate) value).atStartOfDay(DateUtil.zoneId).toEpochSecond());
        }
        throw new IllegalArgumentException("can not cast to Date, value : " + value);

    }

    public static Timestamp castToTimestamp(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Calendar) {
            return new Timestamp(((Calendar) value).getTimeInMillis());
        }

        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }

        if (value instanceof Date) {
            return new Timestamp(((Date) value).getTime());
        }

        if (value instanceof Number) {
            long longValue = ((Number) value).longValue();
            return new Timestamp(longValue);
        }

        if (value instanceof String) {
            String strVal = (String) value;
            if (strVal.length() == 0 //
                    || "null".equals(strVal) //
                    || "NULL".equals(strVal)) {
                return null;
            }
            Timestamp date = DateUtil.parseDate(strVal);
            if (date != null) {
                return date;
            }
        } else if (value instanceof LocalDateTime) {
            return Timestamp.from(((LocalDateTime) value).atZone(DateUtil.zoneId).toInstant());
        } else if (value instanceof LocalDate) {
            return Timestamp.from(((LocalDate) value).atStartOfDay(DateUtil.zoneId).toInstant());
        }
        throw new IllegalArgumentException("can not cast to Timestamp, value : " + value);
    }

    public static Long castToLong(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        if (value instanceof String) {
            String strVal = (String) value;
            if (strVal.length() == 0 //
                    || "null".equals(strVal) //
                    || "NULL".equals(strVal)) {
                return null;
            }

            try {
                return Long.parseLong(strVal);
            } catch (NumberFormatException ex) {
                //
            }
        }
        return null;
    }

    public static Integer castToInt(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Integer) {
            return (Integer) value;
        }

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        if (value instanceof String) {
            String strVal = (String) value;

            if (strVal.length() == 0 //
                    || "null".equals(strVal) //
                    || "NULL".equals(strVal)) {
                return null;
            }

            try {
                return Integer.parseInt(strVal);
            } catch (Exception e) {
                return null;
            }
        }

        if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        }

        throw new IllegalArgumentException("can not cast to int, value : " + value);
    }

    public static byte[] castToBytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }

        if (value instanceof String) {
            return ((String) value).getBytes();
        }
        throw new IllegalArgumentException("can not cast to int, value : " + value);
    }

    public static Boolean castToBoolean(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Boolean) {
            return (Boolean) value;
        }

        if (value instanceof Number) {
            return ((Number) value).intValue() == 1;
        }

        if (value instanceof String) {
            String strVal = (String) value;

            if (strVal.length() == 0 //
                    || "null".equals(strVal) //
                    || "NULL".equals(strVal)) {
                return null;
            }

            if ("true".equalsIgnoreCase(strVal) //
                    || "1".equals(strVal)) {
                return Boolean.TRUE;
            }

            if ("false".equalsIgnoreCase(strVal) //
                    || "0".equals(strVal)) {
                return Boolean.FALSE;
            }

            if ("Y".equalsIgnoreCase(strVal) //
                    || "T".equals(strVal)) {
                return Boolean.TRUE;
            }

            if ("F".equalsIgnoreCase(strVal) //
                    || "N".equals(strVal)) {
                return Boolean.FALSE;
            }
        }

        throw new IllegalArgumentException("can not cast to boolean, value : " + value);
    }

    /**
     * 转换类型
     *
     * @param obj   需要转换的对象
     * @param clazz 转换至类型
     * @param <T>   类型
     * @return 转换后的对象
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> T cast(Object obj, Class<T> clazz) {
        if (obj == null) {
            if (clazz == int.class) {
                return (T) Integer.valueOf(0);
            } else if (clazz == long.class) {
                return (T) Long.valueOf(0);
            } else if (clazz == short.class) {
                return (T) Short.valueOf((short) 0);
            } else if (clazz == byte.class) {
                return (T) Byte.valueOf((byte) 0);
            } else if (clazz == float.class) {
                return (T) Float.valueOf(0);
            } else if (clazz == double.class) {
                return (T) Double.valueOf(0);
            } else if (clazz == boolean.class) {
                return (T) Boolean.FALSE;
            }
            return null;
        }

        if (clazz == null) {
            throw new IllegalArgumentException("clazz is null");
        }

        if (clazz == obj.getClass()) {
            return (T) obj;
        }
        if (clazz.isAssignableFrom(obj.getClass())) {
            return (T) obj;
        }

        if (clazz.isArray()) {
            if (obj instanceof Collection) {
                Collection collection = (Collection) obj;
                int index = 0;
                Object array = Array.newInstance(clazz.getComponentType(), collection.size());
                for (Object item : collection) {
                    Object value = cast(item, clazz.getComponentType());
                    Array.set(array, index, value);
                    index++;
                }

                return (T) array;
            }

            if (clazz == byte[].class) {
                return (T) castToBytes(obj);
            }
        }
        if (clazz == boolean.class || clazz == Boolean.class) {
            return (T) castToBoolean(obj);
        }

        if (clazz == byte.class || clazz == Byte.class) {
            return (T) castToByte(obj);
        }

        if (clazz == char.class || clazz == Character.class) {
            return (T) castToChar(obj);
        }

        if (clazz == short.class || clazz == Short.class) {
            return (T) castToShort(obj);
        }

        if (clazz == int.class || clazz == Integer.class) {
            return (T) castToInt(obj);
        }

        if (clazz == long.class || clazz == Long.class) {
            return (T) castToLong(obj);
        }

        if (clazz == float.class || clazz == Float.class) {
            return (T) castToFloat(obj);
        }

        if (clazz == double.class || clazz == Double.class) {
            return (T) castToDouble(obj);
        }

        if (clazz == String.class) {
            return (T) castToString(obj);
        }

        if (clazz == BigDecimal.class) {
            return (T) castToBigDecimal(obj);
        }

        if (clazz == BigInteger.class) {
            return (T) castToBigInteger(obj);
        }

        if (clazz == Date.class) {
            return (T) castToDate(obj);
        }

        if (clazz == java.sql.Date.class) {
            return (T) castToSqlDate(obj);
        }

        if (clazz == Timestamp.class) {
            return (T) castToTimestamp(obj);
        }

        if (clazz.isEnum()) {
            return castToEnum(obj, clazz);
        }

        if (Calendar.class.isAssignableFrom(clazz)) {
            Date date = castToDate(obj);
            Calendar calendar;
            if (clazz == Calendar.class) {
                calendar = Calendar.getInstance();
            } else {
                try {
                    calendar = (Calendar) clazz.newInstance();
                } catch (Exception e) {
                    throw new IllegalArgumentException("can not cast to : " + clazz.getName(), e);
                }
            }
            calendar.setTime(date);
            return (T) calendar;
        }

        if (obj instanceof String) {
            String strVal = (String) obj;

            if (strVal.length() == 0 //
                    || "null".equals(strVal) //
                    || "NULL".equals(strVal)) {
                return null;
            }

            if (clazz == Currency.class) {
                return (T) Currency.getInstance(strVal);
            }

            if (clazz == Locale.class) {
                return (T) toLocale(strVal);
            }
        }
        return BeanUtil.transform(obj, clazz, BeanUtil.EMPTY_IDENTITY_HASH_MAP);
//        throw new IllegalArgumentException("can not cast to : " + clazz.getName());
    }

    public static Locale toLocale(String strVal) {
        String[] items = strVal.split("_");

        if (items.length == 1) {
            return new Locale(items[0]);
        }

        if (items.length == 2) {
            return new Locale(items[0], items[1]);
        }

        return new Locale(items[0], items[1], items[2]);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> T castToEnum(Object obj, Class<T> clazz) {
        try {
            if (obj instanceof String) {
                String name = (String) obj;
                if (name.length() == 0) {
                    return null;
                }

                char charAt0 = name.charAt(0);
                if (charAt0 >= '0' && charAt0 <= '9') {
                    obj = Integer.valueOf(name);
                } else {
                    return (T) Enum.valueOf((Class<? extends Enum>) clazz, name);
                }
            }

            if (obj instanceof Number) {
                int ordinal = ((Number) obj).intValue();
                Object[] values = clazz.getEnumConstants();
                if (ordinal < values.length) {
                    return (T) values[ordinal];
                }
            }
        } catch (Exception ex) {
            throw new IllegalArgumentException("can not cast to : " + clazz.getName(), ex);
        }

        throw new IllegalArgumentException("can not cast to : " + clazz.getName());
    }

    @SuppressWarnings("unchecked")
    public static <T> T cast(Object obj, Type type) {
        if (obj == null) {
            return null;
        }

        if (type instanceof Class) {
            return cast(obj, (Class<T>) type);
        }

        if (type instanceof ParameterizedType) {
            return cast(obj, (ParameterizedType) type);
        }

        if (obj instanceof String) {
            String strVal = (String) obj;
            if (strVal.length() == 0 //
                    || "null".equals(strVal) //
                    || "NULL".equals(strVal)) {
                return null;
            }
        }

        if (type instanceof TypeVariable) {
            return (T) obj;
        }

        throw new IllegalArgumentException("can not cast to : " + type);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> T cast(Object obj, ParameterizedType type) {
        Type rawTye = type.getRawType();

        if (rawTye == Set.class || rawTye == HashSet.class //
                || rawTye == TreeSet.class //
                || rawTye == List.class //
                || rawTye == ArrayList.class) {
            Type itemType = type.getActualTypeArguments()[0];

            if (obj instanceof Iterable) {
                Collection collection;
                if (rawTye == Set.class || rawTye == HashSet.class) {
                    collection = new HashSet();
                } else if (rawTye == TreeSet.class) {
                    collection = new TreeSet();
                } else {
                    collection = new ArrayList();
                }

                for (Iterator it = ((Iterable) obj).iterator(); it.hasNext(); ) {
                    Object item = it.next();
                    collection.add(cast(item, itemType));
                }

                return (T) collection;
            }
        }

        if (rawTye == Map.class || rawTye == HashMap.class) {
            Type keyType = type.getActualTypeArguments()[0];
            Type valueType = type.getActualTypeArguments()[1];

            if (obj instanceof Map) {
                Map map = new HashMap();

                for (Map.Entry entry : ((Map<?, ?>) obj).entrySet()) {
                    Object key = cast(entry.getKey(), keyType);
                    Object value = cast(entry.getValue(), valueType);

                    map.put(key, value);
                }

                return (T) map;
            }
        }

        if (obj instanceof String) {
            String strVal = (String) obj;
            if (strVal.length() == 0) {
                return null;
            }
        }

        if (type.getActualTypeArguments().length == 1) {
            Type argType = type.getActualTypeArguments()[0];
            if (argType instanceof WildcardType) {
                return cast(obj, rawTye);
            }
        }

        throw new IllegalArgumentException("can not cast to : " + type);
    }


}
