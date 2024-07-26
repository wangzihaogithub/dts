package com.github.dts.util;

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
     * @param configuration 外部适配器配置信息
     * @param envProperties 环境变量的配置属性
     */
    void init(CanalConfig.CanalAdapter canalAdapter, CanalConfig.OuterAdapterConfig configuration, Properties envProperties);

    /**
     * 往适配器中同步数据
     *
     * @param dmls 数据包
     * @return 同步结束后则完毕
     */
    CompletableFuture<Void> sync(List<Dml> dmls);

    /**
     * 外部适配器销毁接口
     */
    void destroy();

    CanalConfig.OuterAdapterConfig getConfiguration();

    default String[] getDestination() {
        return getConfiguration().getCanalAdapter().getDestination();
    }

    default String getClientIdentity() {
        return getConfiguration().getCanalAdapter().clientIdentity();
    }
}
