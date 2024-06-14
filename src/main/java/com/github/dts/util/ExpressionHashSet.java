package com.github.dts.util;

import java.util.Collection;
import java.util.HashSet;

/**
 * 表达式HashSet
 *
 * @author acer01
 */
public class ExpressionHashSet extends HashSet<String> {
    public ExpressionHashSet() {
    }

    public ExpressionHashSet(Collection<? extends String> c) {
        super(c);
    }

    @Override
    public boolean contains(Object o) {
        if (super.contains("*")) {
            return true;
        }
        return super.contains(o);
    }
}
