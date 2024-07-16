package com.github.dts.util;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;

public interface MetaDataRepository {

    static MetaDataRepository newInstance(String key, BeanFactory beanFactory) {
        if (PlatformDependentUtil.REDIS_CONNECTION_FACTORY_CLASS != null) {
            Object redisConnectionFactory;
            try {
                redisConnectionFactory = beanFactory.getBean("redisConnectionFactory");
            } catch (BeansException e) {
                redisConnectionFactory = beanFactory.getBean(PlatformDependentUtil.REDIS_CONNECTION_FACTORY_CLASS);
            }
            return new RedisMetaDataRepository(
                    key,
                    redisConnectionFactory);
        } else {
            return null;
        }
    }

    <T> T getCursor();

    void setCursor(Object cursor);

    default void close() {
    }

}
