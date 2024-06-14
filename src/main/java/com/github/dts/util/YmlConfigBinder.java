package com.github.dts.util;

import org.springframework.util.PropertyPlaceholderHelper;
import org.springframework.util.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 将yaml内容绑定到指定对象, 遵循spring yml的绑定规范
 *
 * @author reweerma 2019-2-1 上午9:14:02
 * @version 1.0.0
 */
public class YmlConfigBinder {

    /**
     * 将当前内容指定前缀部分绑定到指定对象并用环境变量中的属性替换占位符, 例: 当前内容有属性 zkServers: ${zookeeper.servers}
     * 在envProperties中有属性 zookeeper.servers:
     * 192.168.0.1:2181,192.168.0.1:2181,192.168.0.1:2181 则当前内容 zkServers 会被替换为
     * zkServers: 192.168.0.1:2181,192.168.0.1:2181,192.168.0.1:2181 注: 假设绑定的类中
     * zkServers 属性是 List<String> 对象, 则会自动映射成List
     *
     * @param prefix       指定前缀
     * @param contentBytes yml内容
     * @param clazz        指定对象类型
     * @return 对象
     */
    public static <T> T bindYmlToObj(String prefix, byte[] contentBytes, Class<T> clazz,
                                     Properties baseProperties) {
        try {
            YamlPropertySourceLoader propertySourceLoader = new YamlPropertySourceLoader();
            Resource configResource = new ByteArrayResource(contentBytes);
            PropertySource<?> propertySource = propertySourceLoader.load("manualBindConfig", configResource, null);

            if (propertySource == null) {
                return null;
            }

            Properties properties = new Properties();
            Map<String, Object> propertiesRes = new LinkedHashMap<>();
            if (!StringUtils.isEmpty(prefix) && !prefix.endsWith(".")) {
                prefix = prefix + ".";
            }

            properties.putAll((Map<?, ?>) propertySource.getSource());

            if (baseProperties != null) {
                baseProperties.putAll(properties);
                properties = baseProperties;
            }

            for (Map.Entry<?, ?> entry : ((Map<?, ?>) propertySource.getSource()).entrySet()) {
                String key = (String) entry.getKey();
                Object value = entry.getValue();

                if (prefix != null) {
                    if (key != null && key.startsWith(prefix)) {
                        key = key.substring(prefix.length());
                    } else {
                        continue;
                    }
                }

                if (value instanceof String && ((String) value).contains("${") && ((String) value).contains("}")) {
                    PropertyPlaceholderHelper propertyPlaceholderHelper = new PropertyPlaceholderHelper("${", "}");
                    value = propertyPlaceholderHelper.replacePlaceholders((String) value, properties);
                }

                propertiesRes.put(key, value);
            }

            if (propertiesRes.isEmpty()) {
                return null;
            }

            propertySource = new MapPropertySource(propertySource.getName(), propertiesRes);

            T target = clazz.newInstance();

            MutablePropertySources propertySources = new MutablePropertySources();
            propertySources.addFirst(propertySource);

            PropertiesConfigurationFactory<Object> factory = new PropertiesConfigurationFactory<Object>(target);
            factory.setPropertySources(propertySources);
            factory.setIgnoreInvalidFields(true);
            factory.setIgnoreUnknownFields(true);

            factory.bindPropertiesToTarget();

            return target;
        } catch (Exception e) {
            Util.sneakyThrows(e);
            return null;
        }
    }
}
