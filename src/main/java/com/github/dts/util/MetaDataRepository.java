package com.github.dts.util;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.env.Environment;

public interface MetaDataRepository {

    static MetaDataRepository newInstance(String key, String redisConnectionFactoryBeanName, BeanFactory beanFactory) {
        if (!PlatformDependentUtil.isSupportSpringframeworkRedis()) {
            return null;
        }
        if (Util.isDefaultRedisProps(beanFactory.getBean(Environment.class))) {
            return null;
        }
        return new LazyMetaDataRepository(() -> {
            Object redisConnectionFactory = SpringUtil.getBean(beanFactory, redisConnectionFactoryBeanName, PlatformDependentUtil.REDIS_CONNECTION_FACTORY_CLASS);
            if (RedisMetaDataRepository.isActive(redisConnectionFactory)) {
                return new RedisMetaDataRepository(key, redisConnectionFactory);
            }
            return null;
        });
    }

    <T> T getCursor();

    void setCursor(Object cursor);

    default void close() {
    }

    default String name() {
        return getClass().getSimpleName();
    }
}
