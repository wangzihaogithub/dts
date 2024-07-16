package com.github.dts.util;

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

}
