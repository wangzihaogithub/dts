package com.github.dts.util;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;

public class SpringUtil {
    public static Object getBean(BeanFactory beanFactory, String beanName, Class type) {
        Object redisConnectionFactory;
        try {
            redisConnectionFactory = beanFactory.getBean(beanName);
        } catch (BeansException e) {
            redisConnectionFactory = beanFactory.getBean(type);
        }
        return redisConnectionFactory;
    }
}
