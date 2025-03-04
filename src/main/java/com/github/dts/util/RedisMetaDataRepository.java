package com.github.dts.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.nio.charset.Charset;

public class RedisMetaDataRepository implements MetaDataRepository {
    private static final Logger log = LoggerFactory.getLogger(RedisMetaDataRepository.class);
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private final byte[] key;
    private final RedisTemplate<byte[], byte[]> redisTemplate = new RedisTemplate<>();

    public RedisMetaDataRepository(String key, Object redisConnectionFactory) {
        this.key = key.getBytes(UTF_8);
        redisTemplate.setKeySerializer(RedisSerializer.byteArray());
        redisTemplate.setValueSerializer(RedisSerializer.byteArray());
        redisTemplate.setConnectionFactory((RedisConnectionFactory) redisConnectionFactory);
        redisTemplate.afterPropertiesSet();
    }

    public static boolean isActive(Object redisConnectionFactory) {
        if (redisConnectionFactory instanceof RedisConnectionFactory) {
            try {
                RedisConnection connection = ((RedisConnectionFactory) redisConnectionFactory).getConnection();
                connection.close();
                return true;
            } catch (Exception e) {
                log.warn("RedisConnection is not active {}", e.toString());
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public <T> T getCursor() {
        try {
            byte[] bytes = redisTemplate.opsForValue().get(key);
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            Data data = JsonUtil.toBeanThrows(new String(bytes, UTF_8), Data.class);
            Class<T> type = (Class<T>) Class.forName(data.getClassName());
            return JsonUtil.toBeanThrows(data.getValue(), type);
        } catch (Exception e) {
            log.warn("getCursor fail {}", e.toString(), e);
            return null;
        }
    }

    @Override
    public void setCursor(Object cursor) {
        try {
            if (cursor == null) {
                redisTemplate.delete(key);
            } else {
                Data data = new Data(cursor.getClass().getName(), JsonUtil.toJson(cursor));
                redisTemplate.opsForValue().set(key, JsonUtil.toJsonUTF8Bytes(data));
            }
        } catch (Exception e) {
            log.warn("setCursor fail {}", e.toString(), e);
        }
    }

    public static class Data {
        private String className;
        private String value;

        public Data() {
        }

        public Data(String className, String value) {
            this.className = className;
            this.value = value;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
