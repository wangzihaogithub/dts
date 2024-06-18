package com.github.dts.util;

/**
 * Holder containing one or more {@link PropertySource} objects.
 *
 * @author Chris Beams
 * @since 3.1
 */
public interface PropertySources extends Iterable<PropertySource<?>> {

    boolean contains(String name);

    PropertySource<?> get(String name);

}
