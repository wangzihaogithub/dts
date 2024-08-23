package com.github.dts.cluster;

import com.github.dts.util.CanalConfig;

public abstract class SdkInstanceClient {
    /**
     * 集群中的下标
     */
    private final int index;
    /**
     * 集群实例总数量
     */
    private final int total;

    private final SdkInstance sdkInstance;
    /**
     * 网络是否可以连上
     */
    private final boolean socketConnected;
    private final CanalConfig.ClusterConfig clusterConfig;

    public SdkInstanceClient(int index,
                             int total,
                             boolean socketConnected,
                             SdkInstance sdkInstance, CanalConfig.ClusterConfig clusterConfig) {
        this.index = index;
        this.total = total;
        this.socketConnected = socketConnected;
        this.sdkInstance = sdkInstance;
        this.clusterConfig = clusterConfig;
    }

    public CanalConfig.ClusterConfig getClusterConfig() {
        return clusterConfig;
    }

    public String getAccount() {
        return sdkInstance.getAccount();
    }

    public boolean isSocketConnected() {
        return socketConnected;
    }

    public int getTotal() {
        return total;
    }

    public int getIndex() {
        return index;
    }

    public SdkInstance getSdkInstance() {
        return sdkInstance;
    }

    public abstract void write(SdkMessage data);

    public abstract void flush();

    public void close() {

    }

    @Override
    public String toString() {
        return "SdkInstanceClient{" +
                index + "/" + total +
                ", socketConnected=" + socketConnected +
                '}';
    }

}
