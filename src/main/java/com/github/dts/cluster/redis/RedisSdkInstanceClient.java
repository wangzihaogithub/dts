package com.github.dts.cluster.redis;

import com.github.dts.cluster.SdkInstance;
import com.github.dts.cluster.SdkInstanceClient;
import com.github.dts.cluster.SdkMessage;
import com.github.dts.cluster.SdkSubscriber;
import com.github.dts.util.CanalConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

public class RedisSdkInstanceClient extends SdkInstanceClient {
    private static final Logger log = LoggerFactory.getLogger(RedisSdkInstanceClient.class);
    private final RedisTemplate<byte[], byte[]> redisTemplate;
    private final SdkSubscriber sdkSubscriber;
    private final byte[] keyIdMsgBytes;
    private Long lastIncrement;
    private long localMessageIdIncrement = 0;

    public RedisSdkInstanceClient(int index, int total, boolean socketConnected, SdkInstance sdkInstance,
                                  CanalConfig.ClusterConfig clusterConfig, RedisTemplate<byte[], byte[]> redisTemplate,
                                  SdkSubscriber sdkSubscriber, byte[] keyIdMsgBytes) {
        super(index, total, socketConnected, sdkInstance, clusterConfig);
        this.keyIdMsgBytes = keyIdMsgBytes;
        this.redisTemplate = redisTemplate;
        this.sdkSubscriber = sdkSubscriber;
    }

    @Override
    public void write(SdkMessage data) {
        if (data.id == null) {
            data.id = incrementId();
        }
        String account = getAccount();
        if (sdkSubscriber.isOpen(account)) {
            sdkSubscriber.write(account, data);
        } else {
            // redis pub
        }
    }

    private long incrementId() {
        Long lastIncrement = this.lastIncrement;
        try {
            int messageIdIncrementDelta = getClusterConfig().getRedis().getMessageIdIncrementDelta();
            if (lastIncrement == null || localMessageIdIncrement > messageIdIncrementDelta) {
                lastIncrement = redisTemplate.opsForValue().increment(keyIdMsgBytes, messageIdIncrementDelta);
                if (lastIncrement != null) {
                    localMessageIdIncrement = 0L;
                    this.lastIncrement = lastIncrement = lastIncrement - messageIdIncrementDelta;
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
    public void flush() {
        String account = getAccount();
        sdkSubscriber.flush(account);
    }
}
