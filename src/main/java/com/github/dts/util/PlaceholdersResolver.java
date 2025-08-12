package com.github.dts.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.*;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertySourcesPropertyResolver;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * 占位符解析
 *
 * @author hao 2021-09-07
 */
public class PlaceholdersResolver {
    private static PlaceholdersResolver INSTANCE;
    private ResourceLoader resourceLoader;
    private ConfigurableEnvironment configurableEnvironment;

    public static String readString(InputStream inputStream, Charset charset) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, charset))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append('\n');
            }
            if (sb.length() > 0) {
                sb.setLength(sb.length() - 1);
            }
            return sb.toString();
        } catch (IOException e) {
            Util.sneakyThrows(e);
            return null;
        }
    }

    public static synchronized PlaceholdersResolver getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new PlaceholdersResolver();
        }
        return INSTANCE;
    }

    @Autowired(required = false)
    public void setConfigurableEnvironment(ConfigurableEnvironment configurableEnvironment) {
        this.configurableEnvironment = configurableEnvironment;
    }

    @Autowired(required = false)
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @PostConstruct
    public void init() {
        INSTANCE = this;
    }

    public String resolve(Resource template, Object metadata) {
        String string;
        try {
            string = readString(template.getInputStream(), Charset.forName("UTF-8"));
        } catch (IOException e) {
            Util.sneakyThrows(e);
            return null;
        }
        return resolve(string, metadata);
    }

    public boolean existResolve(String template) {
        if (template == null) {
            return false;
        }
        int beginIndex = template.indexOf("${");
        return beginIndex != -1 && template.indexOf("}", beginIndex) != -1;
    }

    private Resource getResource(String key) {
        if (resourceLoader == null) {
            resourceLoader = new DefaultResourceLoader();
        }
        if (key.startsWith("/") || key.startsWith("classpath:")) {
            return resourceLoader.getResource(key);
        }
        return null;
    }

    public String resolve(String template, Object metadata) {
        if (template == null) {
            return null;
        }
        Map map = BeanMap.toMap(metadata);
        MutablePropertySources propertySources = new MutablePropertySources();
        PropertySourcesPropertyResolver resolver = new PropertySourcesPropertyResolver(propertySources) {

            @Override
            protected String getPropertyAsRawString(String key) {
                Resource resource = getResource(key);
                if (resource != null) {
                    try {
                        return readString(resource.getInputStream(), Charset.forName("UTF-8"));
                    } catch (IOException e) {
                        Util.sneakyThrows(e);
                        return null;
                    }
                }
                String[] keys = key.split("[.]");
                if (keys.length == 1) {
                    return getProperty(key, String.class, true);
                } else {
                    Object value = map.get(keys[0]);
                    Map value2Map = BeanMap.toMap(value);
                    for (int i = 1; i < keys.length; i++) {
                        value = value2Map.get(keys[i]);
                        if (value == null) {
                            break;
                        }
                        if (i != keys.length - 1) {
                            value2Map = BeanMap.toMap(value);
                        }
                    }
                    if (value == null) {
                        return getProperty(key, String.class, true);
                    }
                    return String.valueOf(value);
                }
            }

            @Override
            protected void logKeyFound(String key, PropertySource<?> propertySource, Object value) {

            }
        };
        propertySources.addLast(new MapPropertySource(map.getClass().getSimpleName(), map));
        if (configurableEnvironment != null) {
            for (PropertySource<?> propertySource : configurableEnvironment.getPropertySources()) {
                propertySources.addLast(propertySource);
            }
            resolver.setConversionService(configurableEnvironment.getConversionService());
        }
        return resolver.resolvePlaceholders(template);
    }
}
