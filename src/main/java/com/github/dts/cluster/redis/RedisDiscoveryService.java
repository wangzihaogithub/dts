package com.github.dts.cluster.redis;

import com.github.dts.cluster.*;
import com.github.dts.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.security.Principal;
import java.util.*;
import java.util.concurrent.*;

public class RedisDiscoveryService implements DiscoveryService, DisposableBean {
    public static final String DEVICE_ID = String.valueOf(SnowflakeIdWorker.INSTANCE.nextId());
    private static final Logger log = LoggerFactory.getLogger(RedisDiscoveryService.class);
    private static final int MIN_REDIS_INSTANCE_EXPIRE_SEC = 2;
    private static volatile ScheduledExecutorService scheduled;
    private final int redisInstanceExpireSec;
    private final byte[] keyServerPubSubBytes;
    private final byte[] keyServerPubUnsubBytes;
    private final byte[] keySdkMqSubBytes;
    private final byte[] keySdkMqUnsubBytes;
    private final byte[] keyServerSetBytes;
    private final ScanOptions keyServerSetScanOptions;
    private final ScanOptions keySdkSetScanOptions;
    private final MessageListener messageServerListener;
    private final MessageListener messageSdkListener;
    private final Jackson2JsonRedisSerializer<ServerInstance> instanceServerSerializer = new Jackson2JsonRedisSerializer<>(ServerInstance.class);
    private final Jackson2JsonRedisSerializer<SdkInstance> instanceSdkSerializer = new Jackson2JsonRedisSerializer<>(SdkInstance.class);
    private final CanalConfig.ClusterConfig clusterConfig;
    private final RedisTemplate<byte[], byte[]> redisTemplate = new RedisTemplate<>();
    private final ServerInstance serverInstance = new ServerInstance();
    private final Collection<ServerListener> serverListenerList = new CopyOnWriteArrayList<>();
    private final Collection<SdkListener> sdkListenerList = new CopyOnWriteArrayList<>();
    private final SdkSubscriber sdkSubscriber;
    private final byte[] serverInstanceBytes;
    private final RedisMessageIdIncrementer messageIdIncrementer;
    private final int updateInstanceTimerMs;
    private long serverHeartbeatCount;
    private Map<String, ServerInstance> serverInstanceMap = new LinkedHashMap<>();
    private Map<String, SdkInstance> sdkInstanceMap = new LinkedHashMap<>();
    // 首次=0，从0开始计数
    private int serverUpdateInstanceCount = 0;
    // 首次=0，从0开始计数
    private int sdkUpdateInstanceCount = 0;
    private ScheduledFuture<?> serverHeartbeatScheduledFuture;
    private boolean destroy;
    private volatile ReferenceCounted<List<RedisServerInstanceClient>> serverInstanceClientListRef = new ReferenceCounted<>(Collections.emptyList());
    private volatile ReferenceCounted<List<RedisSdkInstanceClient>> sdkInstanceClientListRef = new ReferenceCounted<>(Collections.emptyList());
    private ScheduledFuture<?> updateServerInstanceScheduledFuture;
    private ScheduledFuture<?> updateSdkInstanceScheduledFuture;

    public RedisDiscoveryService(Object redisConnectionFactory,
                                 String groupName,
                                 String redisKeyRootPrefix,
                                 int redisInstanceExpireSec,
                                 SdkSubscriber sdkSubscriber,
                                 CanalConfig.ClusterConfig clusterConfig,
                                 String ip,
                                 Integer port) {
        String shortGroupName = String.valueOf(Math.abs(groupName.hashCode()));
        if (shortGroupName.length() >= groupName.length()) {
            shortGroupName = groupName;
        }
        String account = Util.filterNonAscii(shortGroupName + "-" + DEVICE_ID);
        this.serverInstance.setDeviceId(DEVICE_ID);
        this.serverInstance.setAccount(account);
        this.serverInstance.setPassword(UUID.randomUUID().toString().replace("-", ""));
        this.serverInstance.setIp(ip);
        this.serverInstance.setPort(port);
        this.serverInstanceBytes = instanceServerSerializer.serialize(serverInstance);

        this.sdkSubscriber = sdkSubscriber;
        this.updateInstanceTimerMs = clusterConfig.getRedis().getUpdateInstanceTimerMs();
        this.redisInstanceExpireSec = Math.max(redisInstanceExpireSec, MIN_REDIS_INSTANCE_EXPIRE_SEC);
        this.clusterConfig = clusterConfig;
        StringRedisSerializer keySerializer = StringRedisSerializer.UTF_8;
        byte[] keyIdMsgBytes = keySerializer.serialize(redisKeyRootPrefix + "id:msg");
        this.keyServerSetBytes = keySerializer.serialize(redisKeyRootPrefix + "svr:ls:" + DEVICE_ID);
        this.keyServerPubSubBytes = keySerializer.serialize(redisKeyRootPrefix + "svr:mq:sub");
        this.keyServerPubUnsubBytes = keySerializer.serialize(redisKeyRootPrefix + "svr:mq:unsub");
        this.keyServerSetScanOptions = ScanOptions.scanOptions()
                .count(20)
                .match(redisKeyRootPrefix + "svr:ls:*")
                .build();
        this.keySdkMqSubBytes = keySerializer.serialize(redisKeyRootPrefix + "sdk:mq:sub");
        this.keySdkMqUnsubBytes = keySerializer.serialize(redisKeyRootPrefix + "sdk:mq:unsub");
        this.keySdkSetScanOptions = ScanOptions.scanOptions()
                .count(20)
                .match(redisKeyRootPrefix + "sdk:ls:*")
                .build();

        this.messageServerListener = (message, pattern) -> {
            if (this.destroy) {
                return;
            }
            byte[] channel = message.getChannel();
            if (Arrays.equals(channel, keyServerPubSubBytes)) {
                onServerInstanceOnline(instanceServerSerializer.deserialize(message.getBody()));
            } else if (Arrays.equals(channel, keyServerPubUnsubBytes)) {
                onServerInstanceOffline(instanceServerSerializer.deserialize(message.getBody()));
            }
        };
        this.messageSdkListener = (message, pattern) -> {
            if (this.destroy) {
                return;
            }
            byte[] channel = message.getChannel();
            if (Arrays.equals(channel, keySdkMqSubBytes)) {
                onSdkInstanceOnline(instanceSdkSerializer.deserialize(message.getBody()));
            } else if (Arrays.equals(channel, keySdkMqUnsubBytes)) {
                onSdkInstanceOffline(instanceSdkSerializer.deserialize(message.getBody()));
            }
        };

        this.redisTemplate.setConnectionFactory((RedisConnectionFactory) redisConnectionFactory);
        this.redisTemplate.afterPropertiesSet();
        this.messageIdIncrementer = new RedisMessageIdIncrementer(keyIdMsgBytes, clusterConfig.getRedis().getMessageIdIncrementDelta(), redisTemplate);

        // sdk : sub, get
        subscribeSdk();
    }

    private static ScheduledExecutorService getScheduled() {
        if (scheduled == null) {
            synchronized (RedisDiscoveryService.class) {
                if (scheduled == null) {
                    scheduled = new ScheduledThreadPoolExecutor(1, r -> {
                        Thread result = new Thread(r, "RedisDiscoveryServiceHeartbeat");
                        result.setDaemon(true);
                        return result;
                    });
                }
            }
        }
        return scheduled;
    }

    private static boolean isLocalDevice(ServerInstance instance) {
        return Objects.equals(instance.getDeviceId(), DEVICE_ID);
    }

    protected void subscribeSdk() {
        // sdk : sub, get
        Map<String, SdkInstance> sdkInstanceMap = redisTemplate.execute(connection -> {
            connection.subscribe(messageSdkListener, keySdkMqSubBytes, keySdkMqUnsubBytes);
            return getSdkInstanceMap(connection);
        }, true);
        updateSdkInstance(sdkInstanceMap);
    }

    @Override
    public Principal loginServer(String authorization) {
        if (authorization == null || !authorization.startsWith("Basic ")) {
            return null;
        }
        String token = authorization.substring("Basic ".length());
        String[] accountAndPassword = new String(Base64.getDecoder().decode(token)).split(":", 2);
        if (accountAndPassword.length != 2) {
            return null;
        }
        String account = accountAndPassword[0];
        String password = accountAndPassword[1];
        ServerInstance instance = serverInstanceMap.get(account);
        if (instance == null) {
            return null;
        }
        String dbPassword = instance.getPassword();
        if (Objects.equals(dbPassword, password)) {
            return () -> account;
        }
        return null;
    }

    @Override
    public Principal loginSdk(String authorization) {
        if (authorization == null || !authorization.startsWith("Basic ")) {
            log.info("loginSdk fail by authorization is '{}'", authorization);
            return null;
        }
        String token = authorization.substring("Basic ".length());
        String[] accountAndPassword;
        try {
            accountAndPassword = new String(Base64.getDecoder().decode(token)).split(":", 2);
        } catch (Exception e) {
            log.info("loginSdk fail by base64 error {}, {}", authorization, e.toString());
            return null;
        }
        if (accountAndPassword.length != 2) {
            log.info("loginSdk fail by accountAndPassword.length != 2 {}", authorization);
            return null;
        }
        String account = accountAndPassword[0];
        String password = accountAndPassword[1];
        SdkInstance instance = sdkInstanceMap.get(account);
        if (instance == null) {
            log.info("loginSdk fail by account is not exist. account={}", account);
            return null;
        }
        String dbPassword = instance.getPassword();
        if (!Objects.equals(dbPassword, password)) {
            log.info("loginSdk fail by password error. account={}", account);
            return null;
        }
        log.info("loginSdk success. account={}", account);
        return () -> account;
    }

    @Override
    public Principal fetchSdk(String authorization) {
        Principal principal = loginSdk(authorization);
        if (principal == null) {
            updateSdkInstance(getSdkInstanceMap());
            principal = loginSdk(authorization);
        }
        return principal;
    }

    public List<RedisServerInstanceClient> newServerInstanceClient(Collection<ServerInstance> instanceList) {
        int size = instanceList.size();
        List<RedisServerInstanceClient> list = new ArrayList<>(size);
        int i = 0;
        for (ServerInstance instance : instanceList) {
            boolean localDevice = isLocalDevice(instance);
            boolean socketConnected = localDevice || ServerInstance.isSocketConnected(instance, clusterConfig.getTestSocketTimeoutMs());
            try {
                RedisServerInstanceClient service = new RedisServerInstanceClient(i++, size, localDevice, socketConnected, instance, clusterConfig);
                list.add(service);
            } catch (Exception e) {
                throw new IllegalStateException(
                        String.format("newServerInstanceClient  fail!  account = '%s', IP = '%s', port = %d ",
                                instance.getAccount(), instance.getIp(), instance.getPort()), e);
            }
        }
        return list;
    }

    public List<RedisSdkInstanceClient> newSdkInstanceClient(Collection<SdkInstance> instanceList) {
        int size = instanceList.size();
        List<RedisSdkInstanceClient> list = new ArrayList<>(size);
        int i = 0;
        for (SdkInstance instance : instanceList) {
            boolean socketConnected = SdkInstance.isSocketConnected(instance, clusterConfig.getTestSocketTimeoutMs());
            try {
                RedisSdkInstanceClient service = new RedisSdkInstanceClient(i++, size, socketConnected, instance, clusterConfig, sdkSubscriber);
                list.add(service);
            } catch (Exception e) {
                throw new IllegalStateException(
                        String.format("newSdkInstanceClient  fail!  account = '%s', IP = '%s', port = %d ",
                                instance.getAccount(), instance.getIp(), instance.getPort()), e);
            }
        }
        return list;
    }

    @Override
    public void registerServerInstance() {
        // server : set, pub, sub, get
        Map<String, ServerInstance> serverInstanceMap = redisTemplate.execute(connection -> {
            connection.set(keyServerSetBytes, serverInstanceBytes, Expiration.seconds(redisInstanceExpireSec), RedisStringCommands.SetOption.UPSERT);
            connection.publish(keyServerPubSubBytes, serverInstanceBytes);
            connection.subscribe(messageServerListener, keyServerPubSubBytes, keyServerPubUnsubBytes);
            return getServerInstanceMap(connection);
        }, true);
        updateServerInstance(serverInstanceMap);

        scheduledUpdateSdkInstance();
        scheduledUpdateServerInstance();
        scheduledServerHeartbeat();
    }

    @Override
    public ReferenceCounted<List<RedisServerInstanceClient>> getServerListRef() {
        while (true) {
            try {
                return serverInstanceClientListRef.open();
            } catch (IllegalStateException ignored) {

            }
        }
    }

    @Override
    public ReferenceCounted<List<RedisSdkInstanceClient>> getSdkListRef() {
        while (true) {
            try {
                return sdkInstanceClientListRef.open();
            } catch (IllegalStateException ignored) {

            }
        }
    }

    private void scheduledUpdateServerInstance() {
        ScheduledFuture<?> scheduledFuture = this.updateServerInstanceScheduledFuture;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.updateServerInstanceScheduledFuture = null;
        }
        if (updateInstanceTimerMs <= 0) {
            return;
        }
        this.updateServerInstanceScheduledFuture = getScheduled().scheduleWithFixedDelay(() -> updateServerInstance(getServerInstanceMap()), updateInstanceTimerMs, updateInstanceTimerMs, TimeUnit.MILLISECONDS);
    }

    private void scheduledUpdateSdkInstance() {
        ScheduledFuture<?> scheduledFuture = this.updateSdkInstanceScheduledFuture;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.updateSdkInstanceScheduledFuture = null;
        }
        if (updateInstanceTimerMs <= 0) {
            return;
        }
        this.updateSdkInstanceScheduledFuture = getScheduled().scheduleWithFixedDelay(() -> updateSdkInstance(getSdkInstanceMap()), updateInstanceTimerMs, updateInstanceTimerMs, TimeUnit.MILLISECONDS);
    }

    private synchronized void scheduledServerHeartbeat() {
        ScheduledFuture<?> scheduledFuture = this.serverHeartbeatScheduledFuture;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.serverHeartbeatScheduledFuture = null;
        }
        int delay;
        if (redisInstanceExpireSec == MIN_REDIS_INSTANCE_EXPIRE_SEC) {
            delay = 500;
        } else {
            delay = (redisInstanceExpireSec * 1000) / 3;
        }
        this.serverHeartbeatScheduledFuture = getScheduled().scheduleWithFixedDelay(() -> {
            redisTemplate.execute(connection -> {
                // 续期过期时间
                Boolean success = connection.expire(keyServerSetBytes, redisInstanceExpireSec);
                if (success == null || !success) {
                    redisSetServerInstance(connection);
                }
                serverHeartbeatCount++;
                return null;
            }, true);
        }, delay, delay, TimeUnit.MILLISECONDS);
    }

    private Boolean redisSetServerInstance(RedisConnection connection) {
        return connection.set(keyServerSetBytes, serverInstanceBytes, Expiration.seconds(redisInstanceExpireSec), RedisStringCommands.SetOption.UPSERT);
    }

    public synchronized void updateServerInstance(Map<String, ServerInstance> serverInstanceMap) {
        if (serverInstanceMap == null) {
            // serverInstanceMap == null is Redis IOException
            return;
        }
        DifferentComparatorUtil.ListDiffResult<String> diff = DifferentComparatorUtil.listDiff(this.serverInstanceMap.keySet(), serverInstanceMap.keySet());
        if (diff.isEmpty()) {
            return;
        }
        log.info("updateServerInstance insert={}, delete={}", diff.getInsertList(), diff.getDeleteList());
        ReferenceCounted<List<RedisServerInstanceClient>> before = this.serverInstanceClientListRef;
        ReferenceCounted<List<RedisServerInstanceClient>> after = new ReferenceCounted<>(newServerInstanceClient(serverInstanceMap.values()));
        try {
            notifyServerChangeEvent(before, after);
        } catch (Exception e) {
            log.warn("updateServerInstance notifyChangeEvent error {}", e.toString(), e);
        }
        before.destroy(list -> {
            for (ServerInstanceClient service : list) {
                service.close();
            }
        });
        this.serverUpdateInstanceCount++;
        this.serverInstanceClientListRef = after;
        this.serverInstanceMap = serverInstanceMap;
    }

    public synchronized void updateSdkInstance(Map<String, SdkInstance> sdkInstanceMap) {
        if (sdkInstanceMap == null) {
            // sdkInstanceMap == null is Redis IOException
            return;
        }
        DifferentComparatorUtil.ListDiffResult<String> diff = DifferentComparatorUtil.listDiff(this.sdkInstanceMap.keySet(), sdkInstanceMap.keySet());
        if (diff.isEmpty()) {
            return;
        }
        log.info("updateSdkInstance insert={}, delete={}", diff.getInsertList(), diff.getDeleteList());
        ReferenceCounted<List<RedisSdkInstanceClient>> before = this.sdkInstanceClientListRef;
        ReferenceCounted<List<RedisSdkInstanceClient>> after = new ReferenceCounted<>(newSdkInstanceClient(sdkInstanceMap.values()));
        try {
            notifySdkChangeEvent(before, after);
        } catch (Exception e) {
            log.warn("updateSdkInstance notifyChangeEvent error {}", e.toString(), e);
        }
        before.destroy(list -> {
            for (SdkInstanceClient service : list) {
                service.close();
            }
        });
        this.sdkUpdateInstanceCount++;
        this.sdkInstanceClientListRef = after;
        this.sdkInstanceMap = sdkInstanceMap;
    }

    private void notifyServerChangeEvent(ReferenceCounted<List<RedisServerInstanceClient>> before,
                                         ReferenceCounted<List<RedisServerInstanceClient>> after) {
        if (serverListenerList.isEmpty()) {
            return;
        }
        ServerChangeEvent<RedisServerInstanceClient> event = new ServerChangeEvent<>(serverUpdateInstanceCount, before.get(), after.get());
        for (ServerListener listener : serverListenerList) {
            before.open();
            after.open();
            ReferenceCounted<ServerChangeEvent<RedisServerInstanceClient>> ref = new ReferenceCounted<>(event);
            listener.onChange(ref);
            ref.destroy(e -> {
                try {
                    before.close();
                } catch (Exception be) {
                    log.warn("before ServerInstanceClient close error {}", be.toString(), be);
                }
                try {
                    after.close();
                } catch (Exception ae) {
                    log.warn("after ServerInstanceClient close error {}", ae.toString(), ae);
                }
            });
        }
    }

    private void notifySdkChangeEvent(ReferenceCounted<List<RedisSdkInstanceClient>> before,
                                      ReferenceCounted<List<RedisSdkInstanceClient>> after) {
        if (serverListenerList.isEmpty()) {
            return;
        }
        SdkChangeEvent<RedisSdkInstanceClient> event = new SdkChangeEvent<>(sdkUpdateInstanceCount, before.get(), after.get());
        for (SdkListener listener : sdkListenerList) {
            before.open();
            after.open();
            ReferenceCounted<SdkChangeEvent<RedisSdkInstanceClient>> ref = new ReferenceCounted<>(event);
            listener.onChange(ref);
            ref.destroy(e -> {
                try {
                    before.close();
                } catch (Exception be) {
                    log.warn("before SdkInstanceClient close error {}", be.toString(), be);
                }
                try {
                    after.close();
                } catch (Exception ae) {
                    log.warn("after SdkInstanceClient close error {}", ae.toString(), ae);
                }
            });
        }
    }

    private void onSdkInstanceOnline(SdkInstance instance) {
        updateSdkInstance(getSdkInstanceMap());
    }

    private void onSdkInstanceOffline(SdkInstance instance) {
        updateSdkInstance(getSdkInstanceMap());
    }

    private void onServerInstanceOnline(ServerInstance instance) {
        if (!isLocalDevice(instance)) {
            updateServerInstance(getServerInstanceMap());
        }
    }

    private void onServerInstanceOffline(ServerInstance instance) {
        if (!isLocalDevice(instance)) {
            updateServerInstance(getServerInstanceMap());
        }
    }

    public Map<String, ServerInstance> getServerInstanceMap() {
        RedisCallback<Map<String, ServerInstance>> redisCallback = this::getServerInstanceMap;
        return redisTemplate.execute(redisCallback);
    }

    public Map<String, SdkInstance> getSdkInstanceMap() {
        RedisCallback<Map<String, SdkInstance>> redisCallback = this::getSdkInstanceMap;
        return redisTemplate.execute(redisCallback);
    }

    public Map<String, ServerInstance> getServerInstanceMap(RedisConnection connection) {
        Map<String, ServerInstance> map = new LinkedHashMap<>();
        try (Cursor<byte[]> cursor = connection.scan(keyServerSetScanOptions)) {
            while (cursor.hasNext()) {
                byte[] key = cursor.next();
                if (key == null) {
                    continue;
                }
                byte[] body = connection.get(key);
                if (body == null) {
                    continue;
                }
                ServerInstance instance = instanceServerSerializer.deserialize(body);
                if (instance != null) {
                    map.put(instance.getAccount(), instance);
                }
            }
        } catch (Exception ignored) {
            return null;
        }
        return map;
    }

    public Map<String, SdkInstance> getSdkInstanceMap(RedisConnection connection) {
        Map<String, SdkInstance> map = new LinkedHashMap<>();
        try (Cursor<byte[]> cursor = connection.scan(keySdkSetScanOptions)) {
            while (cursor.hasNext()) {
                byte[] key = cursor.next();
                if (key == null) {
                    continue;
                }
                byte[] body = connection.get(key);
                if (body == null) {
                    continue;
                }
                SdkInstance instance = instanceSdkSerializer.deserialize(body);
                if (instance != null) {
                    map.put(instance.getAccount(), instance);
                }
            }
        } catch (Exception ignored) {
            return null;
        }
        return map;
    }

    @Override
    public void destroy() {
        this.destroy = true;
        redisTemplate.execute(connection -> {
            connection.expire(keyServerSetBytes, 0);
            connection.publish(keyServerPubUnsubBytes, serverInstanceBytes);
            return null;
        }, true);
    }

    @Override
    public void addServerListener(ServerListener serverListener) {
        serverListenerList.add(serverListener);
    }

    @Override
    public void addSdkListener(SdkListener serverListener) {
        sdkListenerList.add(serverListener);
    }

    @Override
    public MessageIdIncrementer getMessageIdIncrementer() {
        return messageIdIncrementer;
    }

    private static class RedisMessageIdIncrementer implements MessageIdIncrementer {
        private final int messageIdIncrementDelta;
        private final RedisTemplate<byte[], byte[]> redisTemplate;
        private final byte[] keyIdMsgBytes;
        private Long lastIncrement;
        private long localMessageIdIncrement = 0;
        private int lastIncrementDelta;

        RedisMessageIdIncrementer(byte[] keyIdMsgBytes, int messageIdIncrementDelta, RedisTemplate<byte[], byte[]> redisTemplate) {
            this.keyIdMsgBytes = keyIdMsgBytes;
            this.messageIdIncrementDelta = messageIdIncrementDelta;
            this.redisTemplate = redisTemplate;
        }

        @Override
        public long incrementId() {
            return incrementId(messageIdIncrementDelta);
        }

        @Override
        public long incrementId(int estimatedCount) {
            Long lastIncrement = this.lastIncrement;
            int incrementDelta = Math.max(messageIdIncrementDelta, estimatedCount);
            try {
                if (lastIncrement == null || localMessageIdIncrement > lastIncrementDelta) {
                    lastIncrement = redisTemplate.opsForValue().increment(keyIdMsgBytes, incrementDelta);
                    if (lastIncrement != null) {
                        this.localMessageIdIncrement = 0L;
                        this.lastIncrement = lastIncrement = lastIncrement - incrementDelta;
                        this.lastIncrementDelta = incrementDelta;
                    }
                }
            } catch (Exception e) {
                log.warn("RedisSdkInstanceClient increment ID error {}", e.toString(), e);
            }
            if (lastIncrement == null) {
                return this.lastIncrement = 1L;
            } else {
                return lastIncrement + localMessageIdIncrement++;
            }
        }

        @Override
        public String toString() {
            return "RedisMessageIdIncrementer{" +
                    "messageIdIncrementDelta=" + messageIdIncrementDelta +
                    ", lastIncrement=" + lastIncrement +
                    ", localMessageIdIncrement=" + localMessageIdIncrement +
                    '}';
        }
    }


}
