package com.github.dts.util;

import org.springframework.beans.factory.BeanFactory;

public interface MetaDataRepository {

    Acknowledge NULL_ACK = new Acknowledge() {
        @Override
        public void ack() {

        }

        @Override
        public String toString() {
            return "NULL_ACK";
        }
    };

    static MetaDataRepository newInstance(String key, BeanFactory beanFactory) {
        if (PlatformDependentUtil.REDIS_CONNECTION_FACTORY_CLASS != null) {
            Object redisConnectionFactory = PlatformDependentUtil.getRedisConnectionFactory(beanFactory);
            if (redisConnectionFactory != null) {
                return new RedisMetaDataRepository(
                        key,
                        redisConnectionFactory);
            }
        }
        return null;
    }

    <T> T getCursor();

    void setCursor(Object cursor);

    default void close() {
    }

    interface Acknowledge {
        void ack();
    }

}
