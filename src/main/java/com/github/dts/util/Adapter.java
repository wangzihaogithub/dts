package com.github.dts.util;

import com.github.dts.canal.StartupServer;
import com.github.dts.cluster.DiscoveryService;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * 外部适配器接口
 *
 * @author reweerma 2018-8-18 下午10:14:02
 * @version 1.0.0
 */
public interface Adapter {

    /**
     * 外部适配器初始化接口
     *
     * @param canalAdapter     适配器参数
     * @param configuration    外部适配器配置信息
     * @param envProperties    环境变量的配置属性
     * @param discoveryService 服务发现
     * @param server           server
     */
    void init(CanalConfig.CanalAdapter canalAdapter,
              CanalConfig.OuterAdapterConfig configuration, Properties envProperties,
              DiscoveryService discoveryService, StartupServer server);

    /**
     * 往适配器中同步数据
     *
     * @param dmls            数据包
     * @param adapterListSize 一共有几个在一起跑
     * @return 同步结束后则完毕
     */
    CompletableFuture<Void> sync(List<Dml> dmls, int adapterListSize);

    /**
     * 外部适配器销毁接口
     */
    void destroy();

    CanalConfig.OuterAdapterConfig getConfiguration();

    default String getName() {
        return getConfiguration().getName();
    }

    default String[] getDestination() {
        return getConfiguration().getCanalAdapter().getDestination();
    }

    default String getClientIdentity() {
        return getConfiguration().getCanalAdapter().clientIdentity();
    }
}
