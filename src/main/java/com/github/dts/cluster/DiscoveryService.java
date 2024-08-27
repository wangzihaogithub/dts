package com.github.dts.cluster;

import com.github.dts.cluster.redis.RedisDiscoveryService;
import com.github.dts.util.*;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.env.Environment;

import java.security.Principal;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public interface DiscoveryService extends SdkLoginService {

    static DiscoveryService newInstance(CanalConfig.ClusterConfig config,
                                        SdkSubscriber sdkSubscriber,
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

        Environment env = beanFactory.getBean(Environment.class);
        String ip = Util.getIPAddress();
        Integer port = env.getProperty("server.port", Integer.class, 8080);
        switch (discoveryEnum) {
            case REDIS: {
                CanalConfig.ClusterConfig.Redis redis = config.getRedis();
                String redisKeyRootPrefix = redis.getRedisKeyRootPrefix();
                if (redisKeyRootPrefix != null) {
                    redisKeyRootPrefix = env.resolvePlaceholders(redisKeyRootPrefix);
                }
                Object redisConnectionFactory = SpringUtil.getBean(beanFactory, redis.getRedisConnectionFactoryBeanName(), PlatformDependentUtil.REDIS_CONNECTION_FACTORY_CLASS);
                return new RedisDiscoveryService(
                        redisConnectionFactory,
                        config.getGroupName(),
                        redisKeyRootPrefix,
                        redis.getRedisInstanceExpireSec(),
                        sdkSubscriber,
                        config, ip, port);
            }
            case NACOS:
            default: {
                throw new IllegalArgumentException("ServiceDiscoveryService newInstance fail! remote discovery config is empty!");
            }
        }
    }

    Principal loginServer(String authorization);

    void registerServerInstance();

    <E extends ServerInstanceClient> ReferenceCounted<List<E>> getServerListRef();

    <E extends SdkInstanceClient> ReferenceCounted<List<E>> getSdkListRef();

    <E extends SdkInstanceClient> List<E> getConfigSdkUnmodifiableList();

    void addServerListener(ServerListener serverListener);

    void addSdkListener(SdkListener serverListener);

    MessageIdIncrementer getMessageIdIncrementer();

    interface ServerListener {
        <E extends ServerInstanceClient> void onChange(ReferenceCounted<ServerChangeEvent<E>> event);
    }

    interface SdkListener {
        <E extends SdkInstanceClient> void onChange(ReferenceCounted<SdkChangeEvent<E>> event);
    }

    interface MessageIdIncrementer {
        long incrementId();

        long incrementId(int estimatedCount);
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
