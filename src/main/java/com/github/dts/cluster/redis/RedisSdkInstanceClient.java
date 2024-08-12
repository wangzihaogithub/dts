package com.github.dts.cluster.redis;

import com.github.dts.cluster.SdkInstance;
import com.github.dts.cluster.SdkInstanceClient;
import com.github.dts.util.CanalConfig;

public class RedisSdkInstanceClient extends SdkInstanceClient {
    public RedisSdkInstanceClient(int index, int total, boolean socketConnected, SdkInstance sdkInstance, CanalConfig.ClusterConfig clusterConfig) {
        super(index, total, socketConnected, sdkInstance, clusterConfig);
    }
}
