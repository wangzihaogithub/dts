package com.github.dts.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.LinkedCaseInsensitiveMap;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class Util {
    private static final Logger logger = LoggerFactory.getLogger(Util.class);
    private static final char[] DIGITS_LOWER;
    public static Integer port;
    private static String ipAddress;

    static {
        DIGITS_LOWER = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    }

    public static <E extends Throwable> void sneakyThrows(Throwable t) throws E {
        throw (E) t;
    }

    public static <K, V, E extends Map<K, V>> E trimToSize(E e, BiFunction<Integer, Float, E> newInstance) {
        // 优化空间： size=5的数据，原hashMap的 table[16] => table[4]
        E apply = newInstance.apply((e.size() / 2) + 1, 2.5F);
        for (Map.Entry<K, V> kvEntry : e.entrySet()) {
            // 不用Map.putAll是为了防止table增长
            apply.put(kvEntry.getKey(), kvEntry.getValue());
        }
        return apply;
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

    public static URL getConfDirPath(String subConf) {
        URL url = Util.class.getClassLoader().getResource(subConf);
        if (url == null) {
            throw new RuntimeException("Config dir not exists. = " + subConf);
        }
        return url;
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

    public static <K, V> LinkedHashMap<K, V> newComputeIfAbsentMap(int initialCapacity,
                                                                   float loadFactor,
                                                                   boolean accessOrder,
                                                                   int removeEldestEntry) {
        return new LinkedHashMap<K, V>(initialCapacity, loadFactor, accessOrder) {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                if (removeEldestEntry >= 0) {
                    return size() > removeEldestEntry;
                } else {
                    return false;
                }
            }

            @Override
            public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
                V v;
                if ((v = get(key)) == null) {
                    synchronized (this) {
                        if ((v = get(key)) == null) {
                            V newValue;
                            if ((newValue = mappingFunction.apply(key)) != null) {
                                put(key, newValue);
                                return newValue;
                            }
                        }
                    }
                }
                return v;
            }
        };
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

    /**
     * 将一段范围拆分为n个子范围
     *
     * @param startId 起始ID
     * @param endId   结束ID
     * @param n       拆分段数
     * @return 包含n个子范围的列表
     */
    public static List<Range> splitRange(Long startId, Long endId, int n) {
        // 参数校验
        if (startId == null) {
            throw new IllegalArgumentException("startId不能为null");
        }
        if (endId == null) {
            throw new IllegalArgumentException("endId不能为null");
        }
        List<Range> ranges = new ArrayList<>();
        if (startId > endId) {
            return ranges;
        }

        // 如果只需要一段，直接返回整个范围
        if (n <= 1) {
            ranges.add(new Range(0, 1, startId, endId));
            return ranges;
        }

        // 计算总范围和每段的大小
        long totalRange = endId - startId + 1;
        long segmentSize = totalRange / n;
        long remainder = totalRange % n;

        int rangeCount = 0;
        for (long i = 0, currentStart = startId; i < n; i++) {
            // 前remainder段多分配一个元素，确保均匀分布
            long currentEnd = currentStart + segmentSize - 1;
            if (i < remainder) {
                currentEnd += 1;
            }
            if (currentStart <= currentEnd) {
                rangeCount++;
            }
            currentStart = currentEnd + 1;
        }

        // 拆分为n段
        for (long i = 0, currentStart = startId; i < n; i++) {
            // 前remainder段多分配一个元素，确保均匀分布
            long currentEnd = currentStart + segmentSize - 1;
            if (i < remainder) {
                currentEnd += 1;
            }
            if (currentStart <= currentEnd) {
                ranges.add(new Range((int) i, rangeCount, currentStart, currentEnd));
            }
            currentStart = currentEnd + 1;
        }
        return ranges;
    }

    /**
     * 范围类
     */
    public static class Range {
        private final int index;
        private final int ranges;
        private final Long start;
        private final Long end;

        public Range(int index, int ranges, Long start, Long end) {
            this.index = index;
            this.ranges = ranges;
            this.start = start;
            this.end = end;
        }

        public int getRanges() {
            return ranges;
        }

        public int getIndex() {
            return index;
        }

        public boolean isLast() {
            return ranges - 1 == index;
        }

        public Long getStart() {
            return start;
        }

        public Long getEnd() {
            return end;
        }

        @Override
        public String toString() {
            return index + "/" + ranges + "[" + start + "-" + end + "]";
        }
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
