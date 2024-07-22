package com.github.dts.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

public abstract class PropertySource<T> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final String name;

    protected final T source;

    public PropertySource(String name, T source) {
        Assert.hasText(name, "Property source name must contain at least one character");
        Assert.notNull(source, "Property source must not be null");
        this.name = name;
        this.source = source;
    }

    public PropertySource(String name) {
        this(name, (T) new Object());
    }

    public String getName() {
        return this.name;
    }

    public T getSource() {
        return this.source;
    }

    public boolean containsProperty(String name) {
        return (getProperty(name) != null);
    }

    public abstract Object getProperty(String name);

    @Override
    public boolean equals(Object obj) {
        return (this == obj || (obj instanceof PropertySource
                && ObjectUtils.nullSafeEquals(this.name, ((PropertySource<?>) obj).name)));
    }

    @Override
    public int hashCode() {
        return ObjectUtils.nullSafeHashCode(this.name);
    }

    @Override
    public String toString() {
        if (logger.isDebugEnabled()) {
            return getClass().getSimpleName() + "@" + System.identityHashCode(this) + " {name='" + this.name
                    + "', properties=" + this.source + "}";
        } else {
            return getClass().getSimpleName() + " {name='" + this.name + "'}";
        }
    }

    public static PropertySource<?> named(String name) {
        return new ComparisonPropertySource(name);
    }

    public static class StubPropertySource extends PropertySource<Object> {

        public StubPropertySource(String name) {
            super(name, new Object());
        }

        @Override
        public String getProperty(String name) {
            return null;
        }
    }

    /**
     * @see PropertySource#named(String)
     */
    static class ComparisonPropertySource extends StubPropertySource {

        private static final String USAGE_ERROR = "ComparisonPropertySource instances are for use with collection comparison only";

        public ComparisonPropertySource(String name) {
            super(name);
        }

        @Override
        public Object getSource() {
            throw new UnsupportedOperationException(USAGE_ERROR);
        }

        @Override
        public boolean containsProperty(String name) {
            throw new UnsupportedOperationException(USAGE_ERROR);
        }

        @Override
        public String getProperty(String name) {
            throw new UnsupportedOperationException(USAGE_ERROR);
        }
    }

}
