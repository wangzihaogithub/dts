package com.github.dts.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.PropertyResolver;
import org.springframework.core.env.PropertySourcesPropertyResolver;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Default implementation of the {@link PropertySources} interface. Allows
 * manipulation of contained property sources and provides a constructor for
 * copying an existing {@code PropertySources} instance.
 * <p>
 * Where <em>precedence</em> is mentioned in methods such as {@link #addFirst}
 * and {@link #addLast}, this is with regard to the order in which property
 * sources will be searched when resolving a given property with a
 * {@link PropertyResolver}.
 *
 * @author Chris Beams
 * @author Juergen Hoeller
 * @see PropertySourcesPropertyResolver
 * @since 3.1
 */
public class MutablePropertySources implements PropertySources {

    private final Logger logger;

    private final List<PropertySource<?>> propertySourceList = new CopyOnWriteArrayList<PropertySource<?>>();

    /**
     * Create a new {@link MutablePropertySources}
     * object.
     */
    public MutablePropertySources() {
        this.logger = LoggerFactory.getLogger(getClass());
    }

    public MutablePropertySources(PropertySources propertySources) {
        this();
        for (PropertySource<?> propertySource : propertySources) {
            addLast(propertySource);
        }
    }

    /**
     * Create a new {@link MutablePropertySources}
     * object and inherit the given logger, usually from an enclosing
     * {@link org.springframework.core.env.Environment}.
     */
    MutablePropertySources(Logger logger) {
        this.logger = logger;
    }

    @Override
    public boolean contains(String name) {
        return this.propertySourceList.contains(PropertySource.named(name));
    }

    @Override
    public PropertySource<?> get(String name) {
        int index = this.propertySourceList.indexOf(PropertySource.named(name));
        return (index != -1 ? this.propertySourceList.get(index) : null);
    }

    @Override
    public Iterator<PropertySource<?>> iterator() {
        return this.propertySourceList.iterator();
    }

    public void addFirst(PropertySource<?> propertySource) {
        if (logger.isDebugEnabled()) {
            logger.debug("Adding PropertySource '" + propertySource.getName() + "' with highest search precedence");
        }
        removeIfPresent(propertySource);
        this.propertySourceList.add(0, propertySource);
    }

    public void addLast(PropertySource<?> propertySource) {
        if (logger.isDebugEnabled()) {
            logger.debug("Adding PropertySource '" + propertySource.getName() + "' with lowest search precedence");
        }
        removeIfPresent(propertySource);
        this.propertySourceList.add(propertySource);
    }

    public void addBefore(String relativePropertySourceName, PropertySource<?> propertySource) {
        if (logger.isDebugEnabled()) {
            logger.debug("Adding PropertySource '" + propertySource.getName()
                    + "' with search precedence immediately higher than '" + relativePropertySourceName + "'");
        }
        assertLegalRelativeAddition(relativePropertySourceName, propertySource);
        removeIfPresent(propertySource);
        int index = assertPresentAndGetIndex(relativePropertySourceName);
        addAtIndex(index, propertySource);
    }

    public void addAfter(String relativePropertySourceName, PropertySource<?> propertySource) {
        if (logger.isDebugEnabled()) {
            logger.debug("Adding PropertySource '" + propertySource.getName()
                    + "' with search precedence immediately lower than '" + relativePropertySourceName + "'");
        }
        assertLegalRelativeAddition(relativePropertySourceName, propertySource);
        removeIfPresent(propertySource);
        int index = assertPresentAndGetIndex(relativePropertySourceName);
        addAtIndex(index + 1, propertySource);
    }

    public int precedenceOf(PropertySource<?> propertySource) {
        return this.propertySourceList.indexOf(propertySource);
    }

    public PropertySource<?> remove(String name) {
        if (logger.isDebugEnabled()) {
            logger.debug("Removing PropertySource '" + name + "'");
        }
        int index = this.propertySourceList.indexOf(PropertySource.named(name));
        return (index != -1 ? this.propertySourceList.remove(index) : null);
    }

    /**
     * Replace the property source with the given name with the given property
     * source object.
     *
     * @param name           the name of the property source to find and replace
     * @param propertySource the replacement property source
     * @throws IllegalArgumentException if no property source with the given name is
     *                                  present
     * @see #contains
     */
    public void replace(String name, PropertySource<?> propertySource) {
        if (logger.isDebugEnabled()) {
            logger.debug("Replacing PropertySource '" + name + "' with '" + propertySource.getName() + "'");
        }
        int index = assertPresentAndGetIndex(name);
        this.propertySourceList.set(index, propertySource);
    }

    public int size() {
        return this.propertySourceList.size();
    }

    @Override
    public String toString() {
        return this.propertySourceList.toString();
    }

    /**
     * Ensure that the given property source is not being added relative to itself.
     */
    protected void assertLegalRelativeAddition(String relativePropertySourceName, PropertySource<?> propertySource) {
        String newPropertySourceName = propertySource.getName();
        if (relativePropertySourceName.equals(newPropertySourceName)) {
            throw new IllegalArgumentException(
                    "PropertySource named '" + newPropertySourceName + "' cannot be added relative to itself");
        }
    }

    /**
     * Remove the given property source if it is present.
     */
    protected void removeIfPresent(PropertySource<?> propertySource) {
        this.propertySourceList.remove(propertySource);
    }

    /**
     * Add the given property source at a particular index in the list.
     */
    private void addAtIndex(int index, PropertySource<?> propertySource) {
        removeIfPresent(propertySource);
        this.propertySourceList.add(index, propertySource);
    }

    /**
     * Assert that the named property source is present and return its index.
     *
     * @param name {@linkplain PropertySource#getName() name of the property source}
     *             to find
     * @throws IllegalArgumentException if the named property source is not present
     */
    private int assertPresentAndGetIndex(String name) {
        int index = this.propertySourceList.indexOf(PropertySource.named(name));
        if (index == -1) {
            throw new IllegalArgumentException("PropertySource named '" + name + "' does not exist");
        }
        return index;
    }
}
