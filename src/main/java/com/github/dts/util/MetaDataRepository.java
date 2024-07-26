package com.github.dts.util;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;

public interface MetaDataRepository {

    static MetaDataRepository newInstance(String key, BeanFactory beanFactory) {
        return new LazyMetaDataRepository(() -> {
            if (PlatformDependentUtil.REDIS_CONNECTION_FACTORY_CLASS != null) {
                Object redisConnectionFactory;
                try {
                    redisConnectionFactory = beanFactory.getBean("redisConnectionFactory");
                } catch (BeansException e) {
                    try {
                        redisConnectionFactory = beanFactory.getBean(PlatformDependentUtil.REDIS_CONNECTION_FACTORY_CLASS);
                    } catch (Exception e1) {
                        redisConnectionFactory = null;
                    }
                }
                if (RedisMetaDataRepository.isActive(redisConnectionFactory)) {
                    return new RedisMetaDataRepository(key, redisConnectionFactory);
                }
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
