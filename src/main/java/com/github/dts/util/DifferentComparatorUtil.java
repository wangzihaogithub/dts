package com.github.dts.util;

import org.springframework.core.annotation.AnnotationUtils;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.github.dts.util.BeanUtil.*;

/**
 * 比较两个对象的不同之处
 *
 * @author hao
 * @see #diff(Object, Object)
 */
public class DifferentComparatorUtil {

    public static <E> ListDiffResult<E> listDiff(Collection<E> before, Collection<E> after) {
        return listDiff(before, after, e -> e);
    }

    public static <E, ID> ListDiffResult<E> listDiff(Collection<? extends E> before, Collection<? extends E> after, Function<E, ID> idFunction) {
        if (before == null) {
            before = Collections.emptyList();
        }
        if (after == null) {
            after = Collections.emptyList();
        }
        Map<ID, E> leftMap = before.stream()
                .collect(Collectors.toMap(idFunction, e -> e, (o1, o2) -> o1, LinkedHashMap::new));
        Map<ID, E> rightMap = after.stream()
                .collect(Collectors.toMap(idFunction, e -> e, (o1, o2) -> o1, LinkedHashMap::new));

        ListDiffResult<E> result = new ListDiffResult<>();
        for (Map.Entry<ID, E> entry : leftMap.entrySet()) {
            if (rightMap.containsKey(entry.getKey())) {
            } else {
                result.getDeleteList().add(entry.getValue());
            }
        }
        for (Map.Entry<ID, E> entry : rightMap.entrySet()) {
            if (leftMap.containsKey(entry.getKey())) {
            } else {
                result.getInsertList().add(entry.getValue());
            }
        }
        return result;
    }

    public static List<Diff> diff(Object left, Object right) {
        return diff(left, right, Integer.MAX_VALUE);
    }

    public static List<Diff> diff(Object left, Object right, int maxDepth) {
        List<Diff> diffList = new ArrayList<>();
        String[] beanPackages = BeanUtil.getBeanPackages();
        addDiffDepth(diffList, new ArrayList<>(), true, true, left, right, 0, maxDepth, beanPackages);
        return diffList;
    }

    public static Map<Pair, List<Diff>> diffGroupBy1(Object left, Object right) {
        List<Diff> diffList = diff(left, right);
        return Diff.groupBy1(diffList);
    }

    public static Map<Diff, List<Diff>> diffGroupBy2(Object left, Object right) {
        List<Diff> diffList = diff(left, right);
        return Diff.groupBy2(diffList);
    }

    public static boolean equals(Object left, Object right) {
        boolean equals = Objects.equals(left, right);
        if (equals) {
            return true;
        }
        if (left instanceof String || right instanceof String) {
            return Objects.equals(Objects.toString(left, ""), Objects.toString(right, ""));
        }
        if (left == null || right == null) {
            return false;
        }

        Class<?> leftClass = left.getClass();
        Class<?> rightClass = right.getClass();
        // 用于 BigDecimal 0.0 和0 的比较， 或日期的比较
        if (left instanceof Comparable && right instanceof Comparable) {
            if (leftClass.isAssignableFrom(rightClass)) {
                return ((Comparable) left).compareTo(right) == 0;
            } else if (rightClass.isAssignableFrom(leftClass)) {
                return ((Comparable) right).compareTo(left) == 0;
            }
        }
        return false;
    }

    private static void addDiffDepth(List<Diff> diffList, List<Pair> path,
                                     boolean parentLeftExist, boolean parentRightExist,
                                     Object left, Object right,
                                     int depth, int maxDepth,
                                     String[] beanPackages) {
        if (depth >= maxDepth) {
            if (!equals(left, right)) {
                diffList.add(new Diff(new ArrayList<>(path)));
            }
            return;
        }
        Value prevLeft = null;
        Value prevRight = null;
        if (path.size() > 0) {
            prevLeft = path.get(path.size() - 1).getLeft();
            prevRight = path.get(path.size() - 1).getRight();
        }
        if (path.size() > 1) {
            path.get(path.size() - 2).getLeft().nextValue = prevLeft;
            path.get(path.size() - 2).getRight().nextValue = prevRight;
        }
        if (isBaseType(left) || isBaseType(right)) {
            if (!equals(left, right)) {
                List<Pair> list = new ArrayList<>(path);
                Pair pair = new BaseTypePair(
                        new Value(left, parentLeftExist, left, null, null, null, prevLeft),
                        new Value(right, parentRightExist, right, null, null, null, prevRight));
                list.add(pair);
                diffList.add(new Diff(list));
            }
        } else if (isIterableOrArray(left.getClass()) && isIterableOrArray(right.getClass())) {
            Iterator leftIterator = wrapIterable(left).iterator();
            Iterator rightIterator = wrapIterable(right).iterator();
            int i = 0;
            while (true) {
                boolean leftHasNext = leftIterator.hasNext();
                boolean rightHasNext = rightIterator.hasNext();
                Object leftNext;
                Object rightNext;
                if (leftHasNext && !rightHasNext) {
                    leftNext = leftIterator.next();
                    rightNext = null;
                } else if (!leftHasNext && rightHasNext) {
                    leftNext = null;
                    rightNext = rightIterator.next();
                } else if (leftHasNext) {
                    leftNext = leftIterator.next();
                    rightNext = rightIterator.next();
                } else {
                    break;
                }
                Pair pair = new IterableOrArrayPair(
                        new Value(left, leftHasNext, leftNext, null, null, null, prevLeft),
                        new Value(right, rightHasNext, rightNext, null, null, null, prevRight),
                        i);
                path.add(pair);
                addDiffDepth(diffList, path, leftHasNext, rightHasNext, leftNext, rightNext, depth + 1, maxDepth, beanPackages);
                path.remove(pair);
                i++;
            }
        } else if (isBeanOrMap(left.getClass(), beanPackages) && isBeanOrMap(right.getClass(), beanPackages)) {
            Map leftMap = wrapMap(left);
            Map rightMap = wrapMap(right);
            BeanMap leftBeanMap = leftMap instanceof BeanMap ? (BeanMap) leftMap : null;
            BeanMap rightBeanMap = rightMap instanceof BeanMap ? (BeanMap) rightMap : null;

            Set fieldNameSet = new LinkedHashSet();
            fieldNameSet.addAll(leftMap.keySet());
            fieldNameSet.addAll(rightMap.keySet());

            for (Object fieldName : fieldNameSet) {
                if (!(fieldName instanceof String)) {
                    continue;
                }
                boolean leftHas = leftMap.containsKey(fieldName);
                boolean rightHas = rightMap.containsKey(fieldName);
                Object leftValue = leftMap.get(fieldName);
                Object rightValue = rightMap.get(fieldName);

                PropertyDescriptor leftDescriptor, rightDescriptor;
                Annotation[] leftAnnotations, rightAnnotations;
                List<Annotation> leftAnnotationList = new ArrayList<>(), rightAnnotationList = new ArrayList<>();
                Field leftField = null, rightField = null;
                Method leftReadMethod = null, rightReadMethod = null;

                if (leftBeanMap != null && (leftDescriptor = leftBeanMap.getPropertyDescriptor(fieldName)) != null) {
                    leftAnnotations = BeanMap.getFieldDeclaredAnnotations(leftDescriptor);
                    leftField = BeanMap.getField(leftDescriptor);
                    leftReadMethod = leftDescriptor.getReadMethod();
                    if (leftAnnotations != null) {
                        leftAnnotationList.addAll(Arrays.asList(leftAnnotations));
                    }
                    if (leftReadMethod != null) {
                        leftAnnotationList.addAll(Arrays.asList(leftReadMethod.getDeclaredAnnotations()));
                    }
                }
                if (rightBeanMap != null && (rightDescriptor = rightBeanMap.getPropertyDescriptor(fieldName)) != null) {
                    rightAnnotations = BeanMap.getFieldDeclaredAnnotations(rightDescriptor);
                    rightField = BeanMap.getField(rightDescriptor);
                    rightReadMethod = rightDescriptor.getReadMethod();
                    if (rightAnnotations != null) {
                        rightAnnotationList.addAll(Arrays.asList(rightAnnotations));
                    }
                    if (rightReadMethod != null) {
                        rightAnnotationList.addAll(Arrays.asList(rightReadMethod.getDeclaredAnnotations()));
                    }
                }

                Pair pair = new BeanOrMapPair(
                        new Value(left, leftHas, leftValue, leftAnnotationList.toArray(new Annotation[0]), leftField, leftReadMethod, prevLeft),
                        new Value(right, rightHas, rightValue, leftAnnotationList.toArray(new Annotation[]{}), rightField, rightReadMethod, prevRight),
                        (String) fieldName);
                path.add(pair);
                addDiffDepth(diffList, path, leftHas, rightHas, leftValue, rightValue, depth + 1, maxDepth, beanPackages);
                path.remove(pair);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported diff objectEnum. left = " + left + ", right = " + right);
        }
    }

    private static Map wrapMap(Object beanOrMap) {
        if (beanOrMap instanceof Map) {
            return (Map) beanOrMap;
        }
        return new BeanMap(beanOrMap);
    }

    public interface Pair extends Cloneable {
        Value getLeft();

        void setLeft(Value left);

        Value getRight();

        void setRight(Value right);

        Pair clone();
    }

    public static class ListDiffResult<E> {
        private final List<E> insertList = new ArrayList<>();
        private final List<E> deleteList = new ArrayList<>();

        public List<E> getInsertList() {
            return insertList;
        }

        public List<E> getDeleteList() {
            return deleteList;
        }

        public boolean isEmpty() {
            return insertList.isEmpty() && deleteList.isEmpty();
        }
    }

    public static class Diff implements Cloneable {
        private List<Pair> paths;

        public Diff() {
        }

        public Diff(List<Pair> paths) {
            this.paths = paths;
        }

        public Diff(Pair paths) {
            this.paths = new ArrayList<>(Collections.singletonList(paths));
        }

        public static Map<Pair, List<Diff>> groupBy1(List<Diff> list) {
            Map<Pair, List<Diff>> groupByMap = new LinkedHashMap<>();
            for (Diff diff : list) {
                List<Pair> paths = diff.getPaths();
                List<Diff> diffValues = groupByMap.computeIfAbsent(paths.get(0), e -> new ArrayList<>());
                diffValues.add(new Diff(paths.subList(1, paths.size())));
            }
            return groupByMap;
        }

        public static Map<Diff, List<Diff>> groupBy2(List<Diff> list) {
            Map<Diff, List<Diff>> groupByMap = new LinkedHashMap<>();
            for (Diff diff : list) {
                List<Pair> paths = diff.getPaths();
                Diff key;
                if (paths.size() >= 2) {
                    key = new Diff(Arrays.asList(paths.get(0), paths.get(1)));
                } else {
                    key = new Diff(Collections.singletonList(paths.get(0)));
                }
                List<Diff> diffValues = groupByMap.computeIfAbsent(key, e -> new ArrayList<>());
                diffValues.add(new Diff(paths.subList(key.getPaths().size(), paths.size())));
            }
            return groupByMap;
        }

        public List<Pair> getPaths() {
            return paths;
        }

        public void setPaths(List<Pair> paths) {
            this.paths = paths;
        }

        public int size() {
            return paths.size();
        }

        public Pair get(int index) {
            return paths.get(index);
        }

        public Pair remove(int index) {
            return paths.remove(index);
        }

        public boolean removeIf(Predicate<Pair> filter) {
            return paths.removeIf(filter);
        }

        public Diff subList(int fromIndex, int toIndex) {
            return new Diff(paths.subList(fromIndex, toIndex));
        }

        @Override
        public Diff clone() {
            List<Pair> paths = new ArrayList<>();
            for (Pair path : this.paths) {
                paths.add(path.clone());
            }
            return new Diff(paths);
        }

        @Override
        public String toString() {
            return paths.stream().map(e -> {
                if (e instanceof IterableOrArrayPair) {
                    return "[" + e + "]";
                } else if (e instanceof BeanOrMapPair) {
                    return "." + e;
                } else {
                    return " : " + e;
                }
            }).collect(Collectors.joining());
        }

        public boolean isEmpty() {
            return paths.isEmpty();
        }
    }

    public static class IterableOrArrayPair implements Pair {
        private Value left;
        private Value right;
        private int index;

        public IterableOrArrayPair(Value left, Value right, int index) {
            this.left = Objects.requireNonNull(left);
            this.right = Objects.requireNonNull(right);
            this.left.parent = this;
            this.right.parent = this;
            this.index = index;
        }

        @Override
        public Value getLeft() {
            return left;
        }

        @Override
        public void setLeft(Value left) {
            this.left = left;
        }

        @Override
        public Value getRight() {
            return right;
        }

        @Override
        public void setRight(Value right) {
            this.right = right;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        @Override
        public String toString() {
            return String.valueOf(index);
        }

        @Override
        public Pair clone() {
            return new IterableOrArrayPair(left.clone(), right.clone(), index);
        }
    }

    public static class Value implements Cloneable {
        private Object source;
        private boolean existField;
        private Object data;
        private Annotation[] fieldOrMethodAnnotations;
        private Field field;
        private Method readMethod;
        private Value prevValue;
        private Pair parent;
        private Value nextValue;
        private Map<Class<? extends Annotation>, Map<String, Object>> sourceAnnotationMap = new HashMap<>();
        private Map<Class<? extends Annotation>, Map<String, Object>> fieldAnnotationMap = new HashMap<>();
        private Map<Class<? extends Annotation>, Map<String, Object>> dataAnnotationMap = new HashMap<>();

        public Value(Object source, boolean existField, Object data, Annotation[] fieldAnnotations,
                     Field field, Method readMethod, Value prevValue) {
            this.source = source;
            this.existField = existField;
            this.data = data;
            this.fieldOrMethodAnnotations = fieldAnnotations;
            this.field = field;
            this.readMethod = readMethod;
            this.prevValue = prevValue;
        }

        public static Map<String, Object> getAnnotationMap(Class<? extends Annotation> type, Annotation... annotations) {
            if (annotations != null) {
                for (Annotation annotation : annotations) {
                    if (annotation == null) {
                        continue;
                    }
                    if (annotation.annotationType() == type) {
                        return AnnotationUtils.getAnnotationAttributes(annotation);
                    }
                    Annotation find = AnnotationUtils.getAnnotation(annotation, type);
                    if (find != null) {
                        Map<String, Object> annotationAttributes = AnnotationUtils.getAnnotationAttributes(find);
                        annotationAttributes.putAll(AnnotationUtils.getAnnotationAttributes(annotation));
                        return annotationAttributes;
                    }
                }
            }
            return null;
        }

        public Object getSource() {
            return source;
        }

        public void setSource(Object source) {
            this.source = source;
        }

        public boolean isExistField() {
            return existField;
        }

        public void setExistField(boolean existField) {
            this.existField = existField;
        }

        public Object getData() {
            return data;
        }

        public void setData(Object data) {
            this.data = data;
        }

        public Annotation[] getFieldOrMethodAnnotations() {
            return fieldOrMethodAnnotations;
        }

        public void setFieldOrMethodAnnotations(Annotation[] fieldOrMethodAnnotations) {
            this.fieldOrMethodAnnotations = fieldOrMethodAnnotations;
        }

        public Field getField() {
            return field;
        }

        public void setField(Field field) {
            this.field = field;
        }

        public Method getReadMethod() {
            return readMethod;
        }

        public void setReadMethod(Method readMethod) {
            this.readMethod = readMethod;
        }

        public Value getPrevValue() {
            return prevValue;
        }

        public void setPrevValue(Value prevValue) {
            this.prevValue = prevValue;
        }

        public Pair getParent() {
            return parent;
        }

        public void setParent(Pair parent) {
            this.parent = parent;
        }

        public Value getNextValue() {
            return nextValue;
        }

        public void setNextValue(Value nextValue) {
            this.nextValue = nextValue;
        }

        public Map<Class<? extends Annotation>, Map<String, Object>> getSourceAnnotationMap() {
            return sourceAnnotationMap;
        }

        public void setSourceAnnotationMap(Map<Class<? extends Annotation>, Map<String, Object>> sourceAnnotationMap) {
            this.sourceAnnotationMap = sourceAnnotationMap;
        }

        public Map<Class<? extends Annotation>, Map<String, Object>> getFieldAnnotationMap() {
            return fieldAnnotationMap;
        }

        public void setFieldAnnotationMap(Map<Class<? extends Annotation>, Map<String, Object>> fieldAnnotationMap) {
            this.fieldAnnotationMap = fieldAnnotationMap;
        }

        public Map<Class<? extends Annotation>, Map<String, Object>> getDataAnnotationMap() {
            return dataAnnotationMap;
        }

        public void setDataAnnotationMap(Map<Class<? extends Annotation>, Map<String, Object>> dataAnnotationMap) {
            this.dataAnnotationMap = dataAnnotationMap;
        }

        @Override
        public Value clone() {
            return new Value(source, existField, data, fieldOrMethodAnnotations, field, readMethod, prevValue);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Value) {
                Value that = (Value) obj;
                return Objects.equals(data, that.data) && Objects.equals(source, that.source);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(data, source);
        }

        public boolean isNull() {
            return data == null;
        }

        public boolean isParentBaseType() {
            return parent instanceof BaseTypePair;
        }

        public boolean isParentBeanOrMap() {
            return parent instanceof BeanOrMapPair;
        }

        public boolean isParentIterableOrArray() {
            return parent instanceof IterableOrArrayPair;
        }

        public boolean isIterableOrArray() {
            if (data == null) {
                return false;
            }
            return BeanUtil.isIterableOrArray(data.getClass());
        }

        public boolean isBaseType() {
            if (data == null) {
                return false;
            }
            return BeanUtil.isBaseType(data.getClass());
        }

        public boolean isBeanOrMap() {
            if (data == null) {
                return false;
            }
            return BeanUtil.isBeanOrMap(data.getClass());
        }

        public Class<?> getGenericType() {
            if (field == null) {
                Object data = getData();
                while (data instanceof Iterable) {
                    for (Object next : ((Iterable) data)) {
                        data = next;
                        if (data != null) {
                            break;
                        }
                    }
                }
                return data == null ? null : data.getClass();
            }
            return BeanUtil.getGenericType(field);
        }

        public String getFieldName() {
            if (parent instanceof BeanOrMapPair) {
                return ((BeanOrMapPair) parent).getFieldName();
            } else if (field != null) {
                return field.getName();
            } else if (prevValue != null) {
                return prevValue.getFieldName();
            } else {
                return null;
            }
        }

        public Annotation[] getSourceAnnotations() {
            if (source == null) {
                return null;
            } else if (source.getClass().isArray()) {
                return source.getClass().getComponentType().getDeclaredAnnotations();
            } else if (source instanceof Iterable) {
                if (prevValue != null) {
                    // [null]的情况
                    return Optional.ofNullable(prevValue.getGenericType()).map(Class::getDeclaredAnnotations).orElse(null);
                } else {
                    Object data = source;
                    while (data instanceof Iterable) {
                        for (Object next : ((Iterable) data)) {
                            data = next;
                            if (data != null) {
                                break;
                            }
                        }
                    }
                    if (data == null) {
                        data = this.source;
                    }
                    return data.getClass().getDeclaredAnnotations();
                }
            } else {
                return source.getClass().getDeclaredAnnotations();
            }
        }

        public boolean existFieldAnnotation(Class<? extends Annotation> type) {
            return getFieldAnnotationMap(type) != null;
        }

        public boolean existSourceAnnotation(Class<? extends Annotation> type) {
            return getSourceAnnotationMap(type) != null;
        }

        public Map<String, Object> getSourceAnnotationMap(Class<? extends Annotation> type) {
            return sourceAnnotationMap.computeIfAbsent(type, k -> getAnnotationMap(k, getSourceAnnotations()));
        }

        public Map<String, Object> getFieldAnnotationMap(Class<? extends Annotation> type) {
            return fieldAnnotationMap.computeIfAbsent(type, k -> getAnnotationMap(k, getFieldOrMethodAnnotations()));
        }

        public Map<String, Object> getDataAnnotationMap(Class<? extends Annotation> type) {
            if (data == null) {
                return null;
            }
            return dataAnnotationMap.computeIfAbsent(type, k -> getAnnotationMap(k, data.getClass().getDeclaredAnnotations()));
        }

        @Override
        public String toString() {
            if (data instanceof Date) {
                return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(data);
            }
            return String.valueOf(data);
        }
    }

    public static class BaseTypePair implements Pair {
        private Value left;
        private Value right;

        public BaseTypePair(Value left, Value right) {
            this.left = Objects.requireNonNull(left);
            this.right = Objects.requireNonNull(right);
            this.left.parent = this;
            this.right.parent = this;
        }

        @Override
        public Value getLeft() {
            return left;
        }

        @Override
        public void setLeft(Value left) {
            this.left = left;
        }

        @Override
        public Value getRight() {
            return right;
        }

        @Override
        public void setRight(Value right) {
            this.right = right;
        }

        @Override
        public String toString() {
            return "left=" + left + ",right=" + right;
        }

        @Override
        public Pair clone() {
            return new BaseTypePair(left.clone(), right.clone());
        }
    }

    public static class BeanOrMapPair implements Pair {
        private Value left;
        private Value right;
        private String fieldName;

        public BeanOrMapPair(Value left, Value right, String fieldName) {
            this.left = Objects.requireNonNull(left);
            this.right = Objects.requireNonNull(right);
            this.left.parent = this;
            this.right.parent = this;
            this.fieldName = Objects.requireNonNull(fieldName);
        }

        @Override
        public Value getLeft() {
            return left;
        }

        @Override
        public void setLeft(Value left) {
            this.left = left;
        }

        @Override
        public Value getRight() {
            return right;
        }

        @Override
        public void setRight(Value right) {
            this.right = right;
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public String toString() {
            return fieldName;
        }

        @Override
        public Pair clone() {
            return new BeanOrMapPair(left.clone(), right.clone(), fieldName);
        }
    }

}
