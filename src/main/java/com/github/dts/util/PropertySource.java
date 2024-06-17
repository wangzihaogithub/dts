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

    /**
     * Create a new {@code PropertySource} with the given name and with a new
     * {@code Object} instance as the underlying source.
     * <p>
     * Often useful in testing scenarios when creating anonymous implementations
     * that never query an actual source but rather return hard-coded values.
     */
    @SuppressWarnings("unchecked")
    public PropertySource(String name) {
        this(name, (T) new Object());
    }

    /**
     * Return the name of this {@code PropertySource}
     */
    public String getName() {
        return this.name;
    }

    /**
     * Return the underlying source object for this {@code PropertySource}.
     */
    public T getSource() {
        return this.source;
    }

    /**
     * Return whether this {@code PropertySource} contains the given name.
     * <p>
     * This implementation simply checks for a {@code null} return value from
     * {@link #getProperty(String)}. Subclasses may wish to implement a more
     * efficient algorithm if possible.
     *
     * @param name the property name to find
     */
    public boolean containsProperty(String name) {
        return (getProperty(name) != null);
    }

    /**
     * Return the value associated with the given name, or {@code null} if not
     * found.
     *
     * @param name the property to find
     */
    public abstract Object getProperty(String name);

    /**
     * This {@code PropertySource} object is equal to the given object if:
     * <ul>
     * <li>they are the same instance
     * <li>the {@code name} properties for both objects are equal
     * </ul>
     * <p>
     * No properties other than {@code name} are evaluated.
     */
    @Override
    public boolean equals(Object obj) {
        return (this == obj || (obj instanceof PropertySource
                && ObjectUtils.nullSafeEquals(this.name, ((PropertySource<?>) obj).name)));
    }

    /**
     * Return a hash code derived from the {@code name} property of this
     * {@code PropertySource} object.
     */
    @Override
    public int hashCode() {
        return ObjectUtils.nullSafeHashCode(this.name);
    }

    /**
     * Produce concise output (type and name) if the current log level does not
     * include debug. If debug is enabled, produce verbose output including the hash
     * code of the PropertySource instance and every name/value property pair.
     * <p>
     * This variable verbosity is useful as a property source such as system
     * properties or environment variables may contain an arbitrary number of
     * property pairs, potentially leading to difficult to read exception and log
     * messages.
     *
     */
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

        /**
         * Always returns {@code null}.
         */
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
