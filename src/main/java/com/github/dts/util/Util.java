package com.github.dts.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.LinkedCaseInsensitiveMap;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class Util {

    public final static String timeZone;    // 当前时区
    private static final Logger logger = LoggerFactory.getLogger(Util.class);
    private static final char[] DIGITS_LOWER;
    private static final char[] DIGITS_UPPER;
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

    static {
        DIGITS_LOWER = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
        DIGITS_UPPER = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    }

    public static <E extends Throwable> void sneakyThrows(Throwable t) throws E {
        throw (E) t;
    }

    public static String encodeBasicAuth(String username, String password, Charset charset) {
        String credentialsString = username + ":" + password;
        byte[] encodedBytes = Base64.getEncoder().encode(credentialsString.getBytes(charset));
        return new String(encodedBytes, charset);
    }

    public static String filterNonAscii(String str) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '"') {
                builder.append('\'');
            } else if (c == ':') {
                builder.append('-');
            } else if (c >= 32 && c <= 126) {
                builder.append(c);
            }
        }
        return builder.toString();
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

    public static String getIPAddressPort() {
        if (port != null && port > 0) {
            return getIPAddress() + ":" + port;
        } else {
            return getIPAddress();
        }
    }

    public static String getIPAddress() {
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

    public static ScheduledThreadPoolExecutor newScheduled(int corePoolSize, Supplier<String> name, Consumer<Throwable> exceptionConsumer) {
        AtomicInteger id = new AtomicInteger();
        return new ScheduledThreadPoolExecutor(corePoolSize, r -> {
            String name1;
            try {
                String nameGet = name.get();
                name1 = nameGet + id.incrementAndGet();
            } catch (Exception e) {
                name1 = String.valueOf(name.getClass()) + id.incrementAndGet();
            }
            Thread result = new Thread(() -> {
                try {
                    r.run();
                } catch (Throwable e) {
                    try {
                        exceptionConsumer.accept(e);
                    } catch (Throwable t) {
                        e.printStackTrace();
                    }
                }
            }, name1);
            result.setDaemon(true);
            return result;
        }, (r, exe) -> {
            if (!exe.isShutdown()) {
                try {
                    exe.getQueue().put(r);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        });
    }

    public static ThreadPoolExecutor newFixedThreadPool(int nThreads, long keepAliveTime, String name, boolean wrapper) {
        return newFixedThreadPool(nThreads, nThreads, keepAliveTime, name, wrapper, true);
    }

    public static ThreadPoolExecutor newFixedThreadPool(int core, int nThreads, long keepAliveTime, String name, boolean wrapper, boolean allowCoreThreadTimeOut) {
        return newFixedThreadPool(core, nThreads, keepAliveTime, name, wrapper, allowCoreThreadTimeOut, 0);
    }

    public static ThreadPoolExecutor newFixedThreadPool(int core, int nThreads, long keepAliveTime, String name, boolean wrapper, boolean allowCoreThreadTimeOut, int queues) {
        return newFixedThreadPool(core, nThreads, keepAliveTime, name, wrapper, allowCoreThreadTimeOut, queues, null);
    }

    public static <E extends Runnable> ThreadPoolExecutor newFixedThreadPool(int core, int nThreads, long keepAliveTime, String name, boolean wrapper, boolean allowCoreThreadTimeOut, int queues, BiFunction<E, E, E> mergeFunction) {
        BlockingQueue<Runnable> workQueue = queues == 0 ?
                new SynchronousQueue<>() :
                (queues < 0 ? mergeFunction == null ? new LinkedBlockingQueue<>(Integer.MAX_VALUE) : (BlockingQueue<Runnable>) new MergeLinkedBlockingQueue<>(Integer.MAX_VALUE, mergeFunction)
                        : mergeFunction == null ? new LinkedBlockingQueue<>(queues) : (BlockingQueue<Runnable>) new MergeLinkedBlockingQueue<>(queues, mergeFunction));
        ThreadPoolExecutor executor = new ThreadPoolExecutor(core,
                nThreads,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                workQueue,
                new CustomizableThreadFactory(name) {
                    @Override
                    public Thread newThread(Runnable runnable) {
                        return super.newThread(wrapper ? () -> {
                            try {
                                runnable.run();
                            } catch (Exception e) {
                                logger.warn("error {}", e.toString(), e);
                                throw e;
                            }
                        } : runnable);
                    }
                }, new ThreadPoolExecutor.CallerRunsPolicy());
        executor.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
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

    public static <V> Map<String, V> newLinkedCaseInsensitiveMap() {
        return new LinkedCaseInsensitiveMap<>();
    }

    public static Set<String> newLinkedCaseInsensitiveSet() {
        return Collections.newSetFromMap(new LinkedCaseInsensitiveMap<>());
    }

    public static <V> Map<String, V> newLinkedCaseInsensitiveMap(Map<String, V> map) {
        Map<String, V> map1 = new LinkedCaseInsensitiveMap<>(map.size());
        map1.putAll(map);
        return map1;
    }

    public static Set<String> newLinkedCaseInsensitiveSet(Collection<String> list) {
        Set<String> strings = Collections.newSetFromMap(new LinkedCaseInsensitiveMap<>(list.size()));
        strings.addAll(list);
        return strings;
    }

    public static String md5(byte[] bytes) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] digest = md5.digest(bytes);

            char[] chars = encodeHex(digest, DIGITS_LOWER);
            return new String(chars);
        } catch (NoSuchAlgorithmException e) {
            sneakyThrows(e);
            return null;
        }
    }

    private static char[] encodeHex(byte[] data, char[] toDigits) {
        int l = data.length;
        char[] out = new char[l << 1];
        int i = 0;

        for (int j = 0; i < l; ++i) {
            out[j++] = toDigits[(240 & data[i]) >>> 4];
            out[j++] = toDigits[15 & data[i]];
        }

        return out;
    }

    public static boolean isDefaultRedisProps(Environment env) {
        String[] props = new String[]{
                "spring.redis.url",
                "spring.redis.host",
                "spring.redis.port",
                "spring.redis.database",
                "spring.redis.username",
                "spring.redis.password",
                "spring.redis.cluster.nodes",
                "spring.redis.sentinel.nodes"
        };
        for (String prop : props) {
            if (env.containsProperty(prop)) {
                return false;
            }
        }
        return true;
    }
}
