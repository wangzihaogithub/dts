package com.github.dts.cluster.redis;

import com.github.dts.cluster.SdkInstance;
import com.github.dts.cluster.SdkInstanceClient;
import com.github.dts.cluster.SdkMessage;
import com.github.dts.cluster.SdkSubscriber;
import com.github.dts.util.CanalConfig;

public class RedisSdkInstanceClient extends SdkInstanceClient {

    private final SdkSubscriber sdkSubscriber;

    public RedisSdkInstanceClient(int index, int total, boolean socketConnected, SdkInstance sdkInstance,
                                  CanalConfig.ClusterConfig clusterConfig,
                                  SdkSubscriber sdkSubscriber) {
        super(index, total, socketConnected, sdkInstance, clusterConfig);
        this.sdkSubscriber = sdkSubscriber;
    }

    @Override
    public void write(SdkMessage data) {
        String account = getAccount();
        if (sdkSubscriber.isOpen(account)) {
            sdkSubscriber.write(account, data);
        } else {
            // redis pub
        }
    }

    @Override
    public void flush() {
        String account = getAccount();
        sdkSubscriber.flush(account);
    }
}
