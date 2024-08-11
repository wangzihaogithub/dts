package com.github.dts.cluster;

import com.github.dts.util.*;
import com.sun.net.httpserver.HttpPrincipal;
import org.springframework.beans.factory.ListableBeanFactory;

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
                Object redisConnectionFactory = SpringUtil.getBean(beanFactory, redis.getRedisConnectionFactoryBeanName(), PlatformDependentUtil.REDIS_CONNECTION_FACTORY_CLASS);
                return new RedisDiscoveryService(
                        redisConnectionFactory,
                        config.getGroupName(),
                        redis.getRedisKeyRootPrefix(),
                        redis.getRedisInstanceExpireSec(),
                        config);
            }
            case NACOS:
            default: {
                throw new IllegalArgumentException("ServiceDiscoveryService newInstance fail! remote discovery config is empty!");
            }
        }
    }

    HttpPrincipal login(String authorization);

    void registerInstance(String ip, int port);

    ReferenceCounted<List<ServerInstanceClient>> getClientListRef();

    void addListener(Listener listener);

    interface Listener {
        void onChange(ReferenceCounted<ChangeEvent> event);
    }

    class ChangeEvent {
        // 首次=0，从0开始计数
        public int updateInstanceCount;
        public final List<ServerInstanceClient> before;
        public final List<ServerInstanceClient> after;
        public final ServerInstanceClient beforeLocalDevice;
        public final ServerInstanceClient afterLocalDevice;
        public final DifferentComparatorUtil.ListDiffResult<ServerInstanceClient> diff;

        public ChangeEvent(int updateInstanceCount,
                           List<ServerInstanceClient> before, List<ServerInstanceClient> after) {
            this.updateInstanceCount = updateInstanceCount;
            this.before = before;
            this.after = after;
            List<ServerInstanceClient> beforeConnectedList = before.stream().filter(ServerInstanceClient::isSocketConnected).collect(Collectors.toList());
            List<ServerInstanceClient> afterConnectedList = before.stream().filter(ServerInstanceClient::isSocketConnected).collect(Collectors.toList());
            this.beforeLocalDevice = before.stream().filter(ServerInstanceClient::isLocalDevice).findFirst().orElse(null);
            this.afterLocalDevice = after.stream().filter(ServerInstanceClient::isLocalDevice).findFirst().orElse(null);
            this.diff = DifferentComparatorUtil.listDiff(beforeConnectedList, afterConnectedList, ServerInstanceClient::getAccount);
        }

    }

}
