package com.github.dts.cluster.redis;

import com.github.dts.cluster.ServerInstance;
import com.github.dts.cluster.ServerInstanceClient;
import com.github.dts.util.CanalConfig;

public class RedisServerInstanceClient extends ServerInstanceClient {
    public RedisServerInstanceClient(int index, int total, boolean localDevice, boolean socketConnected, ServerInstance serverInstance, CanalConfig.ClusterConfig clusterConfig) {
        super(index, total, localDevice, socketConnected, serverInstance, clusterConfig);
    }
}
