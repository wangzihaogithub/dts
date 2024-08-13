package com.github.dts.cluster.redis;

import com.github.dts.cluster.*;
import com.github.dts.util.CanalConfig;
import com.github.dts.util.ReferenceCounted;
import com.github.dts.util.SnowflakeIdWorker;
import com.github.dts.util.Util;
import com.sun.net.httpserver.HttpPrincipal;
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

import java.util.*;
import java.util.concurrent.*;

public class RedisDiscoveryService implements DiscoveryService, DisposableBean {
    public static final String DEVICE_ID = String.valueOf(SnowflakeIdWorker.INSTANCE.nextId());
    private static final Logger log = LoggerFactory.getLogger(RedisDiscoveryService.class);
    private static final int MIN_REDIS_INSTANCE_EXPIRE_SEC = 2;
    private static volatile ScheduledExecutorService scheduled;
    private final int redisInstanceExpireSec;
    private final byte[] keyServerSubBytes;
    private final byte[] keyServerPubSubBytes;
    private final byte[] keyServerPubUnsubBytes;
    private final byte[] keySdkSubBytes;
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
    private long serverHeartbeatCount;
    private byte[] serverInstanceBytes;
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

    public RedisDiscoveryService(Object redisConnectionFactory,
                                 String groupName,
                                 String redisKeyRootPrefix,
                                 int redisInstanceExpireSec,
                                 CanalConfig.ClusterConfig clusterConfig) {
        String shortGroupName = String.valueOf(Math.abs(groupName.hashCode()));
        if (shortGroupName.length() >= groupName.length()) {
            shortGroupName = groupName;
        }
        String account = Util.filterNonAscii(shortGroupName + "-" + DEVICE_ID);
        this.serverInstance.setDeviceId(DEVICE_ID);
        this.serverInstance.setAccount(account);
        this.serverInstance.setPassword(UUID.randomUUID().toString().replace("-", ""));

        this.redisInstanceExpireSec = Math.max(redisInstanceExpireSec, MIN_REDIS_INSTANCE_EXPIRE_SEC);
        this.clusterConfig = clusterConfig;
        StringRedisSerializer keySerializer = StringRedisSerializer.UTF_8;
        this.keyServerSetBytes = keySerializer.serialize(redisKeyRootPrefix + "svr:ls:" + DEVICE_ID);
        this.keyServerPubSubBytes = keySerializer.serialize(redisKeyRootPrefix + "svr:mq:sub");
        this.keyServerPubUnsubBytes = keySerializer.serialize(redisKeyRootPrefix + "svr:mq:unsub");
        this.keyServerSubBytes = keySerializer.serialize(redisKeyRootPrefix + "svr:mq:*");
        this.keyServerSetScanOptions = ScanOptions.scanOptions()
                .count(20)
                .match(redisKeyRootPrefix + "svr:ls:*")
                .build();
        byte[] keySdkPubSubBytes = keySerializer.serialize(redisKeyRootPrefix + "sdk:mq:sub");
        byte[] keySdkPubUnsubBytes = keySerializer.serialize(redisKeyRootPrefix + "sdk:mq:unsub");
        this.keySdkSubBytes = keySerializer.serialize(redisKeyRootPrefix + "sdk:mq:*");
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
            if (Arrays.equals(channel, keySdkPubSubBytes)) {
                onSdkInstanceOnline(instanceSdkSerializer.deserialize(message.getBody()));
            } else if (Arrays.equals(channel, keySdkPubUnsubBytes)) {
                onSdkInstanceOffline(instanceSdkSerializer.deserialize(message.getBody()));
            }
        };

        this.redisTemplate.setConnectionFactory((RedisConnectionFactory) redisConnectionFactory);
        this.redisTemplate.afterPropertiesSet();

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
            connection.pSubscribe(messageSdkListener, keySdkSubBytes);
            return getSdkInstanceMap(connection);
        }, true);
        updateSdkInstance(sdkInstanceMap);
    }

    @Override
    public HttpPrincipal loginServer(String authorization) {
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
            return new HttpPrincipal(account, password);
        }
        return null;
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
                RedisSdkInstanceClient service = new RedisSdkInstanceClient(i++, size, socketConnected, instance, clusterConfig, redisTemplate);
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
    public void registerServerInstance(String ip, int port) {
        serverInstance.setIp(ip);
        serverInstance.setPort(port);
        serverInstanceBytes = instanceServerSerializer.serialize(serverInstance);

        // server : set, pub, sub, get
        Map<String, ServerInstance> serverInstanceMap = redisTemplate.execute(connection -> {
            connection.set(keyServerSetBytes, serverInstanceBytes, Expiration.seconds(redisInstanceExpireSec), RedisStringCommands.SetOption.UPSERT);
            connection.publish(keyServerPubSubBytes, serverInstanceBytes);
            connection.pSubscribe(messageServerListener, keyServerSubBytes);
            return getServerInstanceMap(connection);
        }, true);
        updateServerInstance(serverInstanceMap);
        this.serverHeartbeatScheduledFuture = scheduledServerHeartbeat();
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

    private ScheduledFuture<?> scheduledServerHeartbeat() {
        ScheduledFuture<?> scheduledFuture = this.serverHeartbeatScheduledFuture;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        int delay;
        if (redisInstanceExpireSec == MIN_REDIS_INSTANCE_EXPIRE_SEC) {
            delay = 500;
        } else {
            delay = redisInstanceExpireSec * 1000 - 100;
        }
        return getScheduled().scheduleWithFixedDelay(() -> {
            redisTemplate.execute(connection -> {
                // 续期过期时间
                Long ttl = connection.ttl(keyServerSetBytes, TimeUnit.SECONDS);
                if (ttl != null && ttl > 0) {
                    long exp = redisInstanceExpireSec * (serverHeartbeatCount + 2) - ttl;
                    Boolean success = connection.expire(keyServerSetBytes, exp);
                    if (success == null || !success) {
                        redisSetServerInstance(connection);
                    }
                } else {
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

    public void updateServerInstance(Map<String, ServerInstance> serverInstanceMap) {
        if (serverInstanceMap == null) {
            // serverInstanceMap == null is Redis IOException
            return;
        }
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

    public void updateSdkInstance(Map<String, SdkInstance> sdkInstanceMap) {
        if (sdkInstanceMap == null) {
            // sdkInstanceMap == null is Redis IOException
            return;
        }
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
}
