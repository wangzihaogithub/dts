package com.github.dts.cluster;

import com.github.dts.util.*;
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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class RedisDiscoveryService implements DiscoveryService, DisposableBean {
    private static final Logger log = LoggerFactory.getLogger(RedisDiscoveryService.class);
    public static final String DEVICE_ID = String.valueOf(SnowflakeIdWorker.INSTANCE.nextId());
    private static final int MIN_REDIS_INSTANCE_EXPIRE_SEC = 2;
    private static volatile ScheduledExecutorService scheduled;
    private final int redisInstanceExpireSec;
    private final byte[] keySubBytes;
    private final byte[] keyPubSubBytes;
    private final byte[] keyPubUnsubBytes;
    private final byte[] keySetBytes;
    private final ScanOptions keySetScanOptions;
    private final MessageListener messageListener;
    private final Jackson2JsonRedisSerializer<ServerInstance> instanceSerializer = new Jackson2JsonRedisSerializer<>(ServerInstance.class);
    private final CanalConfig.ClusterConfig clusterConfig;
    private final RedisTemplate<byte[], byte[]> redisTemplate = new RedisTemplate<>();
    private final ServerInstance instance = new ServerInstance();
    private long heartbeatCount;
    private byte[] instanceBytes;
    private Map<String, ServerInstance> instanceMap = new LinkedHashMap<>();
    // 首次=0，从0开始计数
    private int updateInstanceCount = 0;
    private ScheduledFuture<?> heartbeatScheduledFuture;
    private final Collection<Listener> listenerList = new CopyOnWriteArrayList<>();
    private boolean destroy;
    private volatile ReferenceCounted<List<ServerInstanceClient>> serverInstanceClientListRef = new ReferenceCounted<>(Collections.emptyList());

    public RedisDiscoveryService(Object redisConnectionFactory,
                                 String redisKeyRootPrefix,
                                 String groupName,
                                 int redisInstanceExpireSec,
                                 CanalConfig.ClusterConfig clusterConfig) {
        String shortGroupName = String.valueOf(Math.abs(groupName.hashCode()));
        if (shortGroupName.length() >= groupName.length()) {
            shortGroupName = groupName;
        }
        String account = Util.filterNonAscii(shortGroupName + "-" + DEVICE_ID);
        this.instance.setDeviceId(DEVICE_ID);
        this.instance.setAccount(account);
        this.instance.setPassword(UUID.randomUUID().toString().replace("-", ""));

        this.redisInstanceExpireSec = Math.max(redisInstanceExpireSec, MIN_REDIS_INSTANCE_EXPIRE_SEC);
        this.clusterConfig = clusterConfig;
        StringRedisSerializer keySerializer = StringRedisSerializer.UTF_8;
        this.keyPubSubBytes = keySerializer.serialize(redisKeyRootPrefix + ":c:sub");
        this.keyPubUnsubBytes = keySerializer.serialize(redisKeyRootPrefix + ":c:unsub");
        this.keySubBytes = keySerializer.serialize(redisKeyRootPrefix + ":c:*");
        this.keySetBytes = keySerializer.serialize(redisKeyRootPrefix + ":d:" + DEVICE_ID);
        this.keySetScanOptions = ScanOptions.scanOptions()
                .count(20)
                .match(redisKeyRootPrefix + ":d:*")
                .build();

        this.messageListener = (message, pattern) -> {
            if (this.destroy) {
                return;
            }
            byte[] channel = message.getChannel();
            if (Arrays.equals(channel, keyPubSubBytes)) {
                onServerInstanceOnline(instanceSerializer.deserialize(message.getBody()));
            } else if (Arrays.equals(channel, keyPubUnsubBytes)) {
                onServerInstanceOffline(instanceSerializer.deserialize(message.getBody()));
            }
        };

        this.redisTemplate.setConnectionFactory((RedisConnectionFactory) redisConnectionFactory);
        this.redisTemplate.afterPropertiesSet();
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

    @Override
    public HttpPrincipal login(String authorization) {
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
        ServerInstance instance = selectInstanceByAccount(account);
        if (instance == null) {
            return null;
        }
        String dbPassword = instance.getPassword();
        if (Objects.equals(dbPassword, password)) {
            return new HttpPrincipal(account, password);
        }
        return null;
    }

    protected ServerInstance selectInstanceByAccount(String account) {
        return instanceMap.get(account);
    }

    public List<ServerInstanceClient> newServerInstanceClient(Collection<ServerInstance> instanceList) {
        int size = instanceList.size();
        List<ServerInstanceClient> list = new ArrayList<>(size);
        int i = 0;
        for (ServerInstance instance : instanceList) {
            boolean localDevice = isLocalDevice(instance);
            boolean socketConnected = localDevice || ServerInstance.isSocketConnected(instance, clusterConfig.getTestSocketTimeoutMs());
            try {
                ServerInstanceClient service = new ServerInstanceClient(i++, size, localDevice, socketConnected, instance, clusterConfig);
                list.add(service);
            } catch (Exception e) {
                throw new IllegalStateException(
                        String.format("newServerInstanceClient  fail!  account = '%s', IP = '%s', port = %d ",
                                instance.getAccount(), instance.getIp(), instance.getPort()), e);
            }
        }
        return list;
    }

    @Override
    public void registerInstance(String ip, int port) {
        instance.setIp(ip);
        instance.setPort(port);
        instanceBytes = instanceSerializer.serialize(instance);

        Map<String, ServerInstance> instanceMap = redisTemplate.execute(connection -> {
            connection.set(keySetBytes, instanceBytes, Expiration.seconds(redisInstanceExpireSec), RedisStringCommands.SetOption.UPSERT);
            connection.publish(keyPubSubBytes, instanceBytes);
            connection.pSubscribe(messageListener, keySubBytes);
            return getInstanceMap(connection);
        }, true);
        updateInstance(instanceMap);
        this.heartbeatScheduledFuture = scheduledHeartbeat();
    }

    private ScheduledFuture<?> scheduledHeartbeat() {
        ScheduledFuture<?> scheduledFuture = this.heartbeatScheduledFuture;
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
                Long ttl = connection.ttl(keySetBytes, TimeUnit.SECONDS);
                if (ttl != null && ttl > 0) {
                    long exp = redisInstanceExpireSec * (heartbeatCount + 2) - ttl;
                    Boolean success = connection.expire(keySetBytes, exp);
                    if (success == null || !success) {
                        redisSetInstance(connection);
                    }
                } else {
                    redisSetInstance(connection);
                }
                heartbeatCount++;
                return null;
            }, true);
        }, delay, delay, TimeUnit.MILLISECONDS);
    }

    private Boolean redisSetInstance(RedisConnection connection) {
        return connection.set(keySetBytes, instanceBytes, Expiration.seconds(redisInstanceExpireSec), RedisStringCommands.SetOption.UPSERT);
    }

    public void updateInstance(Map<String, ServerInstance> instanceMap) {
        ReferenceCounted<List<ServerInstanceClient>> before = this.serverInstanceClientListRef;
        ReferenceCounted<List<ServerInstanceClient>> after = new ReferenceCounted<>(newServerInstanceClient(instanceMap == null ? Collections.emptyList() : instanceMap.values()));
        try {
            notifyChangeEvent(before, after);
        } catch (Exception e) {
            log.warn("updateInstance notifyChangeEvent error {}", e.toString(), e);
        }
        before.destroy(list -> {
            for (ServerInstanceClient service : list) {
                service.close();
            }
        });
        this.updateInstanceCount++;
        this.serverInstanceClientListRef = after;
        this.instanceMap = instanceMap;
    }

    private void notifyChangeEvent(ReferenceCounted<List<ServerInstanceClient>> before,
                                   ReferenceCounted<List<ServerInstanceClient>> after) {
        if (listenerList.isEmpty()) {
            return;
        }
        ChangeEvent event = new ChangeEvent(updateInstanceCount, before.get(), after.get());
        for (Listener listener : listenerList) {
            before.open();
            after.open();
            ReferenceCounted<ChangeEvent> ref = new ReferenceCounted<>(event);
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

    private void onServerInstanceOnline(ServerInstance instance) {
        if (!isLocalDevice(instance)) {
            updateInstance(getInstanceMap());
        }
    }

    private void onServerInstanceOffline(ServerInstance instance) {
        if (!isLocalDevice(instance)) {
            updateInstance(getInstanceMap());
        }
    }

    public Map<String, ServerInstance> getInstanceMap() {
        RedisCallback<Map<String, ServerInstance>> redisCallback = this::getInstanceMap;
        return redisTemplate.execute(redisCallback);
    }

    public Map<String, ServerInstance> getInstanceMap(RedisConnection connection) {
        Map<String, ServerInstance> map = new LinkedHashMap<>();
        try (Cursor<byte[]> cursor = connection.scan(keySetScanOptions)) {
            while (cursor.hasNext()) {
                byte[] key = cursor.next();
                if (key == null) {
                    continue;
                }
                byte[] body = connection.get(key);
                if (body == null) {
                    continue;
                }
                ServerInstance instance = instanceSerializer.deserialize(body);
                if (instance != null) {
                    map.put(instance.getAccount(), instance);
                }
            }
        } catch (Exception ignored) {
            return null;
        }
        return map;
    }

    private static boolean isLocalDevice(ServerInstance instance) {
        return Objects.equals(instance.getDeviceId(), DEVICE_ID);
    }

    @Override
    public void destroy() {
        this.destroy = true;
        redisTemplate.execute(connection -> {
            connection.expire(keySetBytes, 0);
            connection.publish(keyPubUnsubBytes, instanceBytes);
            return null;
        }, true);
    }

    @Override
    public ReferenceCounted<List<ServerInstanceClient>> getClientListRef() {
        return serverInstanceClientListRef;
    }

    @Override
    public void addListener(Listener listener) {
        listenerList.add(listener);
    }
}
