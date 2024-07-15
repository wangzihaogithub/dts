package com.github.dts.util;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;

public class PlatformDependentUtil {
    public static final Class REDIS_CONNECTION_FACTORY_CLASS;

    static {
        Class redisConnectionFactory;
        try {
            redisConnectionFactory = Class.forName("org.springframework.data.redis.connection.RedisConnectionFactory");
        } catch (Throwable e) {
            redisConnectionFactory = null;
        }
        REDIS_CONNECTION_FACTORY_CLASS = redisConnectionFactory;
    }

    public static Object getRedisConnectionFactory(BeanFactory beanFactory) {
        Object redisConnectionFactory;
        try {
            redisConnectionFactory = beanFactory.getBean("redisConnectionFactory");
        } catch (BeansException e) {
            try {
                redisConnectionFactory = beanFactory.getBean(REDIS_CONNECTION_FACTORY_CLASS);
            } catch (Exception e1) {
                redisConnectionFactory = null;
            }
        }
        return redisConnectionFactory;
    }
}
