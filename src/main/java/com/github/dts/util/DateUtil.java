package com.github.dts.util;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;

public class DateUtil {
    public static TimeZone timeZone;
    public static ZoneId zoneId;
    private static final ThreadLocal<Map<String, SimpleDateFormat>> DATE_FORMAT_THREAD_LOCAL = ThreadLocal.withInitial(() -> new HashMap<>(5));

    static {
        setTimeZone(TimeZone.getDefault());
    }

    public static void setTimeZone(TimeZone timeZone) {
        DateUtil.timeZone = timeZone;
        DateUtil.zoneId = timeZone.toZoneId();
    }

    private static Integer[] parseNumber(String fontSize) {
        if (fontSize == null) {
            return new Integer[0];
        }
        List<Integer> result = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < fontSize.length(); i++) {
            char c = fontSize.charAt(i);
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

    public static String dateFormat(Date date, String pattern) {
        Map<String, SimpleDateFormat> dateFormatMap = DATE_FORMAT_THREAD_LOCAL.get();
        SimpleDateFormat dateFormat = dateFormatMap.computeIfAbsent(pattern, SimpleDateFormat::new);
        return dateFormat.format(date);
    }

    public static Timestamp parseDate(String date) {
        return parseDate(date, timeZone);
    }

    public static Timestamp parseDate(String date, TimeZone timeZone) {
        if (date == null || (date = date.trim()).isEmpty()) {
            return null;
        }
        int shotTimestampLength = 10;
        int longTimestampLength = 13;
        if (date.length() == shotTimestampLength || date.length() == longTimestampLength) {
            if (isNumeric(date)) {
                long timestamp = Long.parseLong(date);
                if (date.length() == shotTimestampLength) {
                    timestamp = timestamp * 1000;
                }
                return new Timestamp(timestamp);
            }
        }
        if ("null".equals(date)) {
            return null;
        }
        if ("NULL".equals(date)) {
            return null;
        }
        Integer[] numbers = parseNumber(date);
        if (numbers.length == 0) {
            return null;
        } else {
            if (numbers[0] > 2999 || numbers[0] < 1900) {
                return null;
            }
            if (numbers.length >= 2) {
                if (numbers[1] > 12 || numbers[1] <= 0) {
                    return null;
                }
            }
            if (numbers.length >= 3) {
                if (numbers[2] > 31 || numbers[2] <= 0) {
                    return null;
                }
            }
            if (numbers.length >= 4) {
                if (numbers[3] > 24 || numbers[3] < 0) {
                    return null;
                }
            }
            if (numbers.length >= 5) {
                if (numbers[4] >= 60 || numbers[4] < 0) {
                    return null;
                }
            }
            if (numbers.length >= 6) {
                if (numbers[5] >= 60 || numbers[5] < 0) {
                    return null;
                }
            }
            Calendar calendar = Calendar.getInstance(timeZone);
            calendar.set(Calendar.MONTH, 0);
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            if (numbers.length == 1) {
                calendar.set(Calendar.YEAR, numbers[0]);
            } else if (numbers.length == 2) {
                calendar.set(Calendar.YEAR, numbers[0]);
                calendar.set(Calendar.MONTH, numbers[1] - 1);
            } else if (numbers.length == 3) {
                calendar.set(Calendar.YEAR, numbers[0]);
                calendar.set(Calendar.MONTH, numbers[1] - 1);
                calendar.set(Calendar.DAY_OF_MONTH, numbers[2]);
            } else if (numbers.length == 4) {
                calendar.set(Calendar.YEAR, numbers[0]);
                calendar.set(Calendar.MONTH, numbers[1] - 1);
                calendar.set(Calendar.DAY_OF_MONTH, numbers[2]);
                calendar.set(Calendar.HOUR_OF_DAY, numbers[3]);
            } else if (numbers.length == 5) {
                calendar.set(Calendar.YEAR, numbers[0]);
                calendar.set(Calendar.MONTH, numbers[1] - 1);
                calendar.set(Calendar.DAY_OF_MONTH, numbers[2]);
                calendar.set(Calendar.HOUR_OF_DAY, numbers[3]);
                calendar.set(Calendar.MINUTE, numbers[4]);
            } else {
                calendar.set(Calendar.YEAR, numbers[0]);
                calendar.set(Calendar.MONTH, numbers[1] - 1);
                calendar.set(Calendar.DAY_OF_MONTH, numbers[2]);
                calendar.set(Calendar.HOUR_OF_DAY, numbers[3]);
                calendar.set(Calendar.MINUTE, numbers[4]);
                calendar.set(Calendar.SECOND, numbers[5]);
            }
            return new Timestamp(calendar.getTimeInMillis());
        }
    }
}
