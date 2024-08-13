package com.github.dts.cluster;

import com.github.dts.cluster.redis.RedisDiscoveryService;
import com.github.dts.util.*;
import com.sun.net.httpserver.HttpPrincipal;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.env.Environment;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public interface DiscoveryService {

    static DiscoveryService newInstance(CanalConfig.ClusterConfig config,
                                        ListableBeanFactory beanFactory) {
        CanalConfig.DiscoveryEnum discoveryEnum = config.getDiscovery();
        if (discoveryEnum == CanalConfig.DiscoveryEnum.DISABLE) {
            return null;
        }
        if (discoveryEnum == CanalConfig.DiscoveryEnum.AUTO) {
            if (!Objects.toString(config.getNacos().getServerAddr(), "").isEmpty()) {
                discoveryEnum = CanalConfig.DiscoveryEnum.NACOS;
            } else if (PlatformDependentUtil.isSupportSpringframeworkRedis() && beanFactory.getBeanNamesForType(PlatformDependentUtil.REDIS_CONNECTION_FACTORY_CLASS).length > 0) {
                discoveryEnum = CanalConfig.DiscoveryEnum.REDIS;
            }
        }

        switch (discoveryEnum) {
            case REDIS: {
                CanalConfig.ClusterConfig.Redis redis = config.getRedis();
                String redisKeyRootPrefix = redis.getRedisKeyRootPrefix();
                if (redisKeyRootPrefix != null) {
                    redisKeyRootPrefix = beanFactory.getBean(Environment.class).resolvePlaceholders(redisKeyRootPrefix);
                }
                Object redisConnectionFactory = SpringUtil.getBean(beanFactory, redis.getRedisConnectionFactoryBeanName(), PlatformDependentUtil.REDIS_CONNECTION_FACTORY_CLASS);
                return new RedisDiscoveryService(
                        redisConnectionFactory,
                        config.getGroupName(),
                        redisKeyRootPrefix,
                        redis.getRedisInstanceExpireSec(),
                        config);
            }
            case NACOS:
            default: {
                throw new IllegalArgumentException("ServiceDiscoveryService newInstance fail! remote discovery config is empty!");
            }
        }
    }

    HttpPrincipal loginServer(String authorization);

    void registerServerInstance(String ip, int port);

    <E extends ServerInstanceClient> ReferenceCounted<List<E>> getServerListRef();

    <E extends SdkInstanceClient> ReferenceCounted<List<E>> getSdkListRef();

    void addServerListener(ServerListener serverListener);

    void addSdkListener(SdkListener serverListener);

    interface ServerListener {
        <E extends ServerInstanceClient> void onChange(ReferenceCounted<ServerChangeEvent<E>> event);
    }

    interface SdkListener {
        <E extends SdkInstanceClient> void onChange(ReferenceCounted<SdkChangeEvent<E>> event);
    }

    class ServerChangeEvent<E extends ServerInstanceClient> {
        public final List<E> before;
        public final List<E> after;
        public final E beforeLocalDevice;
        public final E afterLocalDevice;
        public final DifferentComparatorUtil.ListDiffResult<E> diff;
        // 首次=0，从0开始计数
        public int updateInstanceCount;

        public ServerChangeEvent(int updateInstanceCount,
                                 List<E> before, List<E> after) {
            this.updateInstanceCount = updateInstanceCount;
            this.before = before;
            this.after = after;
            List<E> beforeConnectedList = before.stream().filter(ServerInstanceClient::isSocketConnected).collect(Collectors.toList());
            List<E> afterConnectedList = before.stream().filter(ServerInstanceClient::isSocketConnected).collect(Collectors.toList());
            this.beforeLocalDevice = before.stream().filter(ServerInstanceClient::isLocalDevice).findFirst().orElse(null);
            this.afterLocalDevice = after.stream().filter(ServerInstanceClient::isLocalDevice).findFirst().orElse(null);
            this.diff = DifferentComparatorUtil.listDiff(beforeConnectedList, afterConnectedList, E::getAccount);
        }

    }

    class SdkChangeEvent<E extends SdkInstanceClient> {
        public final List<E> before;
        public final List<E> after;
        public final DifferentComparatorUtil.ListDiffResult<E> diff;
        // 首次=0，从0开始计数
        public int updateInstanceCount;

        public SdkChangeEvent(int updateInstanceCount,
                              List<E> before, List<E> after) {
            this.updateInstanceCount = updateInstanceCount;
            this.before = before;
            this.after = after;
            List<E> beforeConnectedList = before.stream().filter(E::isSocketConnected).collect(Collectors.toList());
            List<E> afterConnectedList = before.stream().filter(E::isSocketConnected).collect(Collectors.toList());
            this.diff = DifferentComparatorUtil.listDiff(beforeConnectedList, afterConnectedList, E::getAccount);
        }

    }

}