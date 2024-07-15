package com.github.dts.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.function.Function;

public class StaticMethodAccessor<T> implements Function<T, Object> {
    private final String classMethodName;
    private final Method method;

    public StaticMethodAccessor(String classMethodName, Class<T> type) {
        Method method = null;
        try {
            String[] split = classMethodName.split("#");
            Class<?> clazz = Class.forName(split[0]);
            method = clazz.getDeclaredMethod(split[1], type);
            method.setAccessible(true);
        } catch (Exception e) {
            Util.sneakyThrows(e);
        }
        if (!Modifier.isStatic(method.getModifiers())) {
            throw new IllegalArgumentException("must is static method. " + classMethodName);
        }
        this.classMethodName = classMethodName;
        this.method = method;
    }

    @Override
    public Object apply(T value) {
        try {
            return method.invoke(null, value);
        } catch (IllegalAccessException | InvocationTargetException e) {
            Util.sneakyThrows(e);
            return null;
        }
    }

    @Override
    public String toString() {
        return classMethodName;
    }

}