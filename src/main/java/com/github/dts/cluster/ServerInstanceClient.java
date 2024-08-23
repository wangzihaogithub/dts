package com.github.dts.cluster;

import com.github.dts.util.CanalConfig;

public abstract class ServerInstanceClient {
    /**
     * 集群中的下标
     */
    private final int index;
    /**
     * 集群实例总数量
     */
    private final int total;

    private final ServerInstance serverInstance;
    /**
     * 是否是自己
     */
    private final boolean localDevice;
    /**
     * 网络是否可以连上
     */
    private final boolean socketConnected;
    private final CanalConfig.ClusterConfig clusterConfig;

    public ServerInstanceClient(int index,
                                int total,
                                boolean localDevice,
                                boolean socketConnected,
                                ServerInstance serverInstance, CanalConfig.ClusterConfig clusterConfig) {
        this.index = index;
        this.total = total;
        this.localDevice = localDevice;
        this.socketConnected = socketConnected;
        this.serverInstance = serverInstance;
        this.clusterConfig = clusterConfig;
    }

    public String getAccount() {
        return serverInstance.getAccount();
    }

    public boolean isLocalDevice() {
        return localDevice;
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

    public ServerInstance getServerInstance() {
        return serverInstance;
    }

    public void close() {

    }

    @Override
    public String toString() {
        return "ServerInstanceClient{" +
                index + "/" + total +
                ", localDevice=" + localDevice +
                ", socketConnected=" + socketConnected +
                '}';
    }
}
