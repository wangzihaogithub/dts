package com.github.dts.cluster.redis;

import com.github.dts.cluster.SdkInstance;
import com.github.dts.cluster.SdkInstanceClient;
import com.github.dts.util.CanalConfig;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.CompletableFuture;

public class RedisSdkInstanceClient extends SdkInstanceClient {
    private final RedisTemplate<byte[], byte[]> redisTemplate ;
    public RedisSdkInstanceClient(int index, int total, boolean socketConnected, SdkInstance sdkInstance,
                                  CanalConfig.ClusterConfig clusterConfig, RedisTemplate<byte[], byte[]> redisTemplate) {
        super(index, total, socketConnected, sdkInstance, clusterConfig);
        this.redisTemplate = redisTemplate;
    }

    @Override
    public CompletableFuture<Void> send(AdapterEnum adapterEnum, Object data) {
        if (isSocketConnected()) {
            // request
        } else {
            // redis pub
        }
        return CompletableFuture.completedFuture(null);
    }
}
