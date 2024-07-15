package com.github.dts.util;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.nio.charset.Charset;

public class RedisMetaDataRepository implements MetaDataRepository {
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private final byte[] key;
    private final RedisTemplate<byte[], byte[]> redisTemplate = new RedisTemplate<>();

    public RedisMetaDataRepository(String key, Object redisConnectionFactory) {
        this.key = key.getBytes(UTF_8);
        redisTemplate.setConnectionFactory((RedisConnectionFactory) redisConnectionFactory);
        redisTemplate.afterPropertiesSet();
    }

    @Override
    public <T> T getCursor() {
        byte[] bytes = redisTemplate.opsForValue().get(key);
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        Data data = JsonUtil.toBean(new String(bytes, UTF_8), Data.class);
        try {
            Class<T> type = (Class<T>) Class.forName(data.getClassName());
            return JsonUtil.toBean(data.getValue(), type);
        } catch (ClassNotFoundException e) {
            Util.sneakyThrows(e);
            return null;
        }
    }

    @Override
    public void setCursor(Object cursor) {
        if (cursor == null) {
            redisTemplate.delete(key);
        } else {
            Data data = new Data(cursor.getClass().getName(), JsonUtil.toJson(cursor));
            redisTemplate.opsForValue().set(key, JsonUtil.toJsonUTF8Bytes(data));
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
