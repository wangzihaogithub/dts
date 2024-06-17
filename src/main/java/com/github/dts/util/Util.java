package com.github.dts.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Enumeration;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

public class Util {

    public final static String timeZone;    // 当前时区
    private static final Logger logger = LoggerFactory.getLogger(Util.class);
    public static Integer port;
    private static DateTimeZone dateTimeZone;
    private static String ipAddress;

    static {
        TimeZone localTimeZone = TimeZone.getDefault();
        int rawOffset = localTimeZone.getRawOffset();
        String symbol = "+";
        if (rawOffset < 0) {
            symbol = "-";
        }
        rawOffset = Math.abs(rawOffset);
        int offsetHour = rawOffset / 3600000;
        int offsetMinute = rawOffset % 3600000 / 60000;
        String hour = String.format("%1$02d", offsetHour);
        String minute = String.format("%1$02d", offsetMinute);
        timeZone = symbol + hour + ":" + minute;
        dateTimeZone = DateTimeZone.forID(timeZone);
        TimeZone.setDefault(TimeZone.getTimeZone("GMT" + timeZone));
    }

    public static <E extends Throwable> void sneakyThrows(Throwable t) throws E {
        throw (E) t;
    }

    /**
     * 通过DS执行sql
     */
    public static Object sqlRS(DataSource ds, String sql, Function<ResultSet, Object> fun) {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                return fun.apply(rs);
            }
        } catch (Exception e) {
            logger.error("sqlRs has error, sql: {} ,异常信息：{}", sql, getStackTrace(e));
            Util.sneakyThrows(e);
            return null;
        }
    }

    public static String getStackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        String errMsg = null;
        try {
            throwable.printStackTrace(pw);
            errMsg = sw.toString();
        } catch (Exception e) {

        } finally {
            pw.close();
        }
        return errMsg;
    }

    public static File getConfDirPath(String subConf) {
        URL url = Util.class.getClassLoader().getResource(subConf);
        String path = url.getPath();
        File dir = new File(path);
        if (!dir.exists()) {
            throw new RuntimeException("Config dir not exists. = " + dir);
        }
        return dir;
    }

    public static String cleanColumn(String column) {
        if (column == null) {
            return null;
        }
        if (column.contains("`")) {
            column = column.replaceAll("`", "");
        }

        if (column.contains("'")) {
            column = column.replaceAll("'", "");
        }

        if (column.contains("\"")) {
            column = column.replaceAll("\"", "");
        }

        return column;
    }

    public static boolean isBlank(String str) {
        int strLen;
        if (str != null && (strLen = str.length()) != 0) {
            for (int i = 0; i < strLen; ++i) {
                if (!Character.isWhitespace(str.charAt(i))) {
                    return false;
                }
            }

            return true;
        } else {
            return true;
        }
    }

    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    public static String getIPAddress() {
        if (port != null && port > 0) {
            return getIPAddress0() + ":" + port;
        } else {
            return getIPAddress0();
        }
    }

    public static String getIPAddress0() {
        if (ipAddress != null) {
            return ipAddress;
        } else {
            try {
                Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
                String[] skipNames = {"TAP", "VPN", "UTUN", "VIRBR"};
                while (networkInterfaces.hasMoreElements()) {
                    NetworkInterface networkInterface = networkInterfaces.nextElement();
                    if (networkInterface.isVirtual() && networkInterface.isLoopback()) {
                        continue;
                    }

                    String name = Objects.toString(networkInterface.getName(), "").trim().toUpperCase();
                    String displayName = Objects.toString(networkInterface.getDisplayName(), "").trim().toUpperCase();
                    String netName = name.length() > 0 ? name : displayName;
                    boolean skip = Stream.of(skipNames).anyMatch(netName::contains);
                    if (skip) {
                        continue;
                    }

                    Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                    while (inetAddresses.hasMoreElements()) {
                        InetAddress inetAddress = inetAddresses.nextElement();
                        if (inetAddress.isLoopbackAddress() || !inetAddress.isSiteLocalAddress()) {
                            continue;
                        }
                        String hostAddress = inetAddress.getHostAddress();
                        return ipAddress = hostAddress;
                    }
                }
                // 如果没有发现 non-loopback地址.只能用最次选的方案
                return ipAddress = InetAddress.getLocalHost().getHostAddress();
            } catch (Exception var6) {
                return null;
            }
        }
    }

    public static ThreadPoolExecutor newFixedThreadPool(int nThreads, long keepAliveTime, String name, boolean wrapper) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(nThreads,
                nThreads,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                new CustomizableThreadFactory(name) {
                    @Override
                    public Thread newThread(Runnable runnable) {
                        return super.newThread(new Runnable() {
                            @Override
                            public void run() {
                                if (wrapper) {
                                    try {
                                        runnable.run();
                                    } catch (Exception e) {
                                        logger.warn("error {}", e, e);
                                        throw e;
                                    }
                                } else {
                                    runnable.run();
                                }
                            }
                        });
                    }
                },
                (r, exe) -> {
                    if (!exe.isShutdown()) {
                        try {
                            exe.getQueue().put(r);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                });
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    public static ThreadPoolExecutor newSingleThreadExecutor(long keepAliveTime) {
        return new ThreadPoolExecutor(1,
                1,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                (r, exe) -> {
                    if (!exe.isShutdown()) {
                        try {
                            exe.getQueue().put(r);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                });
    }

    public static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    /**
     * 通用日期时间字符解析
     *
     * @param datetimeStr 日期时间字符串
     * @return Date
     */
    public static Date parseDate(String datetimeStr) {
        if (isEmpty(datetimeStr)) {
            return null;
        }
        if (datetimeStr.startsWith("0000-00-00")) {
            datetimeStr = "1970";
        }
        if (datetimeStr.startsWith("0000/00/00")) {
            datetimeStr = "1970";
        }

        datetimeStr = datetimeStr.trim();
        if (datetimeStr.contains("-")) {
            if (datetimeStr.contains(":")) {
                datetimeStr = datetimeStr.replace(" ", "T");
            }
        } else if (datetimeStr.contains(":")) {
            datetimeStr = "T" + datetimeStr;
        }

        DateTime dateTime = new DateTime(datetimeStr, dateTimeZone);

        return dateTime.toDate();
    }

    public static Date parseDate2(String datetimeStr) {
        if (isEmpty(datetimeStr)) {
            return null;
        }
        try {
            datetimeStr = datetimeStr.trim();
            int len = datetimeStr.length();
            if (datetimeStr.contains("-") && datetimeStr.contains(":") && datetimeStr.contains(".")) {
                // 包含日期+时间+毫秒
                // 取毫秒位数
                int msLen = len - datetimeStr.indexOf(".") - 1;
                StringBuilder ms = new StringBuilder();
                for (int i = 0; i < msLen; i++) {
                    ms.append("S");
                }
                String formatter = "yyyy-MM-dd HH:mm:ss." + ms;

                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatter);
                LocalDateTime dateTime = LocalDateTime.parse(datetimeStr, dateTimeFormatter);
                return Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
            } else if (datetimeStr.contains("-") && datetimeStr.contains(":")) {
                // 包含日期+时间
                // 判断包含时间位数
                int i = datetimeStr.indexOf(":");
                i = datetimeStr.indexOf(":", i + 1);
                String formatter;
                if (i > -1) {
                    formatter = "yyyy-MM-dd HH:mm:ss";
                } else {
                    formatter = "yyyy-MM-dd HH:mm";
                }

                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatter);
                LocalDateTime dateTime = LocalDateTime.parse(datetimeStr, dateTimeFormatter);
                return Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
            } else if (datetimeStr.contains("-")) {
                // 只包含日期
                String formatter = "yyyy-MM-dd";
                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatter);
                LocalDate localDate = LocalDate.parse(datetimeStr, dateTimeFormatter);
                return Date.from(localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
            } else if (datetimeStr.contains(":")) {
                // 只包含时间
                String formatter;
                if (datetimeStr.contains(".")) {
                    // 包含毫秒
                    int msLen = len - datetimeStr.indexOf(".") - 1;
                    StringBuilder ms = new StringBuilder();
                    for (int i = 0; i < msLen; i++) {
                        ms.append("S");
                    }
                    formatter = "HH:mm:ss." + ms;
                } else {
                    // 判断包含时间位数
                    int i = datetimeStr.indexOf(":");
                    i = datetimeStr.indexOf(":", i + 1);
                    if (i > -1) {
                        formatter = "HH:mm:ss";
                    } else {
                        formatter = "HH:mm";
                    }
                }
                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatter);
                LocalTime localTime = LocalTime.parse(datetimeStr, dateTimeFormatter);
                LocalDate localDate = LocalDate.of(1970, Month.JANUARY, 1);
                LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
                return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }

        return null;
    }
}
