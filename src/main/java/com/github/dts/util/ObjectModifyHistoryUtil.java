package com.github.dts.util;/**
 * Created by ITerGet-Tech on 2019/3/25.
 */

import com.github.dts.util.DifferentComparatorUtil.BeanOrMapPair;
import com.github.dts.util.DifferentComparatorUtil.Diff;
import com.github.dts.util.DifferentComparatorUtil.Pair;
import com.github.dts.util.DifferentComparatorUtil.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.dts.util.BeanUtil.isBeanOrMap;
import static com.github.dts.util.BeanUtil.wrapIterable;
import static com.github.dts.util.DifferentComparatorUtil.diff;

public class ObjectModifyHistoryUtil {
    private static final Logger log = LoggerFactory.getLogger(ObjectModifyHistoryUtil.class);

    /**
     * 用户信息友好的变更记录
     * 必须要实体类的字段路径都有注解 {@link }
     *
     * @param before 旧值
     * @param after  新值
     * @return 变更记录
     * //     * @see LogComparer
     */
    public static List<ModifyHistory> compare(Object before, Object after, int maxDepth, Class<? extends Annotation> logClass) {
        if (before == null && after == null) {
            return new ArrayList<>();
        } else if (before == null) {
            before = BeanUtil.newInstance(after.getClass());
        } else if (after == null) {
            after = BeanUtil.newInstance(before.getClass());
        }

        // 1.比较差异
        List<Diff> totalDiffList = diff(before, after, maxDepth);
        Set<Pair> skipFields = new LinkedHashSet<>();
        List<Diff> diffList = totalDiffList.stream()
                // 过滤没打注解的字段
                .filter(e -> {
                    Pair last = e.get(e.size() - 1);
                    Value left = last.getLeft();
                    Value right = last.getRight();
                    if (left.isBaseType() && right.isBaseType()) {
                        return left.existFieldAnnotation(logClass)
                                || right.existFieldAnnotation(logClass)
                                || (left.getPrevValue() != null && left.getPrevValue().existFieldAnnotation(logClass))
                                || (right.getPrevValue() != null && right.getPrevValue().existFieldAnnotation(logClass));
                    }
                    return true;
                })
                // 向上聚合,直到找到注解的那层.如果找不到注解,就丢掉这个字段 (group by)
                .map(e -> {
                    Pair last = e.get(e.size() - 1);
                    Value left = last.getLeft();
                    Value right = last.getRight();

                    boolean existAnnotation = true;
                    int prevCount = 0;
                    while (left != null && right != null
//                            && !left.isNull() && !right.isNull()
                    ) {
                        if (left.existFieldAnnotation(logClass) || right.existFieldAnnotation(logClass)) {
                            break;
                        } else if (left.existSourceAnnotation(logClass) || right.existSourceAnnotation(logClass)) {
                            prevCount++;
                            break;
                        } else {
                            prevCount++;
                            int prevIndex = e.size() - prevCount - 1;
                            if (prevIndex < 0) {
                                existAnnotation = false;
                                break;
                            }
                            skipFields.add(last);
                            last = e.get(prevIndex);
                            left = last.getLeft();
                            right = last.getRight();
                        }
                    }
                    if (existAnnotation) {
                        return e.subList(0, Math.max(e.size() - prevCount, 1));
                    } else {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .map(e -> {
                    Diff clone = e.clone();
                    clone.removeIf(skipFields::contains);
                    return clone;
                })
                .distinct()
                .collect(Collectors.toList());

        // 2.合并相同字段的内容
        Map<Pair, List<Diff>> groupByMap = Diff.groupBy1(diffList);

        // 3.区分类型增删改
        return groupByMap.entrySet().stream()
                .map(e -> {
                    Pair key = e.getKey();
                    Value field;
                    if (key.getLeft().isNull()) {
                        field = key.getRight();
                    } else {
                        field = key.getLeft();
                    }

                    ModifyHistory result = new ModifyHistory(field, logClass, "/");
                    for (Diff diff : e.getValue()) {
                        Diff newDiff = diff.isEmpty() ? new Diff(key) : diff;
                        int lastIndex = newDiff.size() - 1;
                        Pair lastPathPair = newDiff.get(lastIndex);
                        Value left = lastPathPair.getLeft();
                        Value right = lastPathPair.getRight();
                        Object leftItem = left.getData();
                        Object rightItem = right.getData();
//                        boolean handleCollection = false;
//                        if (left.isParentIterableOrArray() && !right.isNull()) {
//                            handleCollection = true;
//                            Collection leftSourceList = wrapCollection(left.getSource());
//                            if (!leftSourceList.contains(rightItem)) {
//                                Diff clone = newDiff.clone();
//                                clone.get(lastIndex).getLeft().setData(null);
//                                result.getInsertList().add(clone);
//                            }
//                        }
//                        if (right.isParentIterableOrArray() && !left.isNull()) {
//                            handleCollection = true;
//                            Collection rightSourceList = wrapCollection(right.getSource());
//                            if (!rightSourceList.contains(leftItem)) {
//                                Diff clone = newDiff.clone();
//                                clone.get(lastIndex).getRight().setData(null);
//                                result.getDeleteList().add(clone);
//                            }
//                        }
//                        if (handleCollection) {
//                            continue;
//                        }
                        if (!isEmpty(leftItem) && !isEmpty(rightItem)) {
                            // 修改
                            result.getUpdateList().add(newDiff);
                        } else if (isEmpty(leftItem) && !isEmpty(rightItem)) {
                            // 新增
                            result.getInsertList().add(newDiff);
                        } else if (!isEmpty(leftItem)) {
                            // 删除
                            result.getDeleteList().add(newDiff);
                        }
                    }
                    return result;
                })
                .filter(e -> !e.isEmpty())
                .sorted()
                .collect(Collectors.toList());
    }

    private static boolean isEmpty(Object object) {
        if (object == null) {
            return true;
        }
        if (object instanceof Collection) {
            return ((Collection) object).isEmpty();
        }
        if (object instanceof Map) {
            return ((Map) object).isEmpty();
        }
        if (object instanceof String) {
            return Util.isBlank((String) object);
        }
        return false;
    }

    public static class ModifyHistory implements Comparable<ModifyHistory> {
        private final String pathDelimiter;
        private final Value field;
        private final Enum objectEnum;
        private final Map<String, Object> fieldAnnotationMap;
        private final Map<String, Object> sourcedAnnotationMap;
        private final List<Diff> insertList = new ArrayList<>();
        private final List<Diff> updateList = new ArrayList<>();
        private final List<Diff> deleteList = new ArrayList<>();
        private final Class<? extends Annotation> logComparerClass;

        private ModifyHistory(Value field, Class<? extends Annotation> logComparerClass, String pathDelimiter) {
            this.field = field;
            this.pathDelimiter = pathDelimiter;
            this.logComparerClass = logComparerClass;
            this.fieldAnnotationMap = field.getFieldAnnotationMap(logComparerClass);
            this.sourcedAnnotationMap = field.getSourceAnnotationMap(logComparerClass);
            this.objectEnum = getAnnotationObjectEnum(getAnnotationMap(), field);
        }

        public static List<Object> flatMap(Object data) {
            List<Object> list = new ArrayList<>(1);
            if (data instanceof Iterable) {
                for (Object next : ((Iterable) data)) {
                    if (next != null) {
                        data = next;
                        list.addAll(flatMap(data));
                    }
                }
            } else {
                list.add(data);
            }
            return list;
        }

        public String getPathDelimiter() {
            return pathDelimiter;
        }

        public Value getField() {
            return field;
        }

        public Enum getObjectEnum() {
            return objectEnum;
        }

        public Map<String, Object> getFieldAnnotationMap() {
            return fieldAnnotationMap;
        }

        public Map<String, Object> getSourcedAnnotationMap() {
            return sourcedAnnotationMap;
        }

        public List<Diff> getInsertList() {
            return insertList;
        }

        public List<Diff> getUpdateList() {
            return updateList;
        }

        public List<Diff> getDeleteList() {
            return deleteList;
        }

        public Class<? extends Annotation> getLogComparerClass() {
            return logComparerClass;
        }

        public Map<String, Object> getAnnotationMap() {
            return fieldAnnotationMap != null ? fieldAnnotationMap : sourcedAnnotationMap;
        }

        public boolean isEmpty() {
            return insertList.isEmpty() && updateList.isEmpty() && deleteList.isEmpty();
        }

        public String geInsertPrefix() {
            return "添加" + getPrefix(getAnnotationMap());
        }

        public String getInsertDisplayText() {
            if (insertList.isEmpty()) {
                return "";
            }
            StringJoiner joiner = new StringJoiner(getAnnotationDelimiter(getAnnotationMap()), geInsertPrefix() + "(", ")");
            for (Diff diff : insertList) {
                String pathNames = getPathNames(diff.getPaths(), pathDelimiter);
                joiner.add(String.format("%s%s%s",
                        pathNames,
                        pathNames.isEmpty() ? "" : ":",
                        toString(diff.get(diff.size() - 1).getRight())));
            }
            return joiner.toString();
        }

        public String getDeletePrefix() {
            return "删除" + getPrefix(getAnnotationMap());
        }

        public String getDeleteDisplayText() {
            if (deleteList.isEmpty()) {
                return "";
            }
            StringJoiner joiner = new StringJoiner(getAnnotationDelimiter(getAnnotationMap()), getDeletePrefix() + "(", ")");
            for (Diff diff : deleteList) {
                String pathNames = getPathNames(diff.getPaths(), pathDelimiter);
                joiner.add(String.format("%s%s%s",
                        pathNames,
                        pathNames.isEmpty() ? "" : ":",
                        toString(diff.get(diff.size() - 1).getLeft())));
            }
            return joiner.toString();
        }

        public String getDiffInsertDisplayText() {
            return insertList.stream()
                    .map(e -> {
                        Pair value = e.get(e.size() - 1);
                        return toString(value.getRight());
                    })
                    .collect(Collectors.joining(getAnnotationDelimiter(getAnnotationMap())));
        }

        public String getDiffDeleteDisplayText() {
            return deleteList.stream()
                    .map(e -> {
                        Pair value = e.get(e.size() - 1);
                        return toString(value.getLeft());
                    })
                    .collect(Collectors.joining(getAnnotationDelimiter(getAnnotationMap())));
        }

        public String getDiffUpdateBeforeDisplayText() {
            return updateList.stream()
                    .map(e -> {
                        Pair value = e.get(e.size() - 1);
                        return toString(value.getLeft());
                    })
                    .collect(Collectors.joining(getAnnotationDelimiter(getAnnotationMap())));
        }

        public String getDiffUpdateAfterDisplayText() {
            return updateList.stream()
                    .map(e -> {
                        Pair value = e.get(e.size() - 1);
                        return toString(value.getRight());
                    })
                    .collect(Collectors.joining(getAnnotationDelimiter(getAnnotationMap())));
        }

        public String getUpdatePrefix() {
            return "编辑" + getPrefix(getAnnotationMap());
        }

        public String getUpdateDisplayText() {
            if (updateList.isEmpty()) {
                return "";
            }
            StringJoiner joiner = new StringJoiner(getAnnotationDelimiter(getAnnotationMap()), getUpdatePrefix() + "(", ")");
            for (Diff diff : updateList) {
                String pathNames = getPathNames(diff.getPaths(), pathDelimiter);

                Pair value = diff.get(diff.size() - 1);
                joiner.add(String.format("%s%s由%s, 修改为%s",
                        pathNames,
                        pathNames.isEmpty() ? "" : ":",
                        toString(value.getLeft()),
                        toString(value.getRight())));
            }
            return joiner.toString();
        }

        private IGPlaceholdersResolver getResolver() {
            IGPlaceholdersResolver resolver = IGPlaceholdersResolver.getInstance();
            return resolver;
        }

        private String getPathNames(List<Pair> pairList, String pathDelimiter) {
            IGPlaceholdersResolver resolver = getResolver();
            return pairList.stream()
                    .filter(e -> e instanceof BeanOrMapPair)
                    .map(e -> (BeanOrMapPair) e)
                    // 防止重复
                    .filter(e -> !Objects.equals(e.getLeft(), field) && !Objects.equals(e.getRight(), field))
                    .map(e -> {
                        Map leftExist = e.getLeft().getFieldAnnotationMap(logComparerClass);
                        if (leftExist != null) {
                            return leftExist;
                        }
                        return e.getRight().getFieldAnnotationMap(logComparerClass);
                    })
                    .filter(Objects::nonNull)
                    .map(this::getAnnotationValue)
                    .filter(e -> !resolver.existResolve(e))
                    .filter(e -> e.length() > 0)
                    .collect(Collectors.joining(pathDelimiter));
        }

        private String toString(Value value) {
            IGPlaceholdersResolver resolver = getResolver();
            Object valueObject = value.getData();
            String result;
            Map<String, Object> annotationMap = null;

            if (value.existFieldAnnotation(logComparerClass)) {
                annotationMap = value.getDataAnnotationMap(logComparerClass);
                if (annotationMap == null) {
                    annotationMap = value.getFieldAnnotationMap(logComparerClass);
                }
            } else if (value.existSourceAnnotation(logComparerClass)) {
                annotationMap = value.getSourceAnnotationMap(logComparerClass);
            }
            Map<String, Object> valueMap = null;
            if (annotationMap != null) {
                valueMap = new LinkedHashMap<>(value.getSource() != null ? new BeanMap(value.getSource()) : new BeanMap());
                valueMap.put("value", valueObject);
                valueMap.put(value.getFieldName(), valueObject);
            }

            if (annotationMap != null && value.isIterableOrArray()) {
                StringJoiner joiner = new StringJoiner(getAnnotationDelimiter(annotationMap));
                Iterable iterable = wrapIterable(valueObject);
                for (Object o : iterable) {
                    Map<String, Object> itemAnnotationMap = null;
                    if (o != null && isBeanOrMap(o.getClass())) {
                        itemAnnotationMap = Value.getAnnotationMap(logComparerClass, o.getClass().getDeclaredAnnotations());
                    }
                    if (itemAnnotationMap == null) {
                        itemAnnotationMap = annotationMap;
                    }
                    String template = getAnnotationValue(itemAnnotationMap);
                    joiner.add(resolver.resolve(template, o));
                }
                result = joiner.toString();
            } else if (annotationMap != null && value.isBeanOrMap()) {
                result = resolver.resolve(getAnnotationValue(annotationMap), valueObject);
            } else if (annotationMap != null && value.isBaseType()) {
                if (valueObject == null) {
                    result = "";
                } else if (valueObject instanceof Date) {
                    result = new SimpleDateFormat(getAnnotationDateFormat(annotationMap)).format(valueObject);
                } else if (valueObject instanceof String) {
                    String template = getAnnotationValue(annotationMap);
                    if (resolver.existResolve(template)) {
                        result = template;
                    } else {
                        result = ((String) valueObject).trim();
                    }
                } else if (valueObject instanceof BigDecimal) {
                    result = ((BigDecimal) valueObject).stripTrailingZeros().toPlainString();
                } else if (valueObject instanceof Boolean) {
                    String booleanFormat;
                    if (Boolean.TRUE.equals(valueObject)) {
                        booleanFormat = getAnnotationBooleanTrue(annotationMap);
                    } else {
                        booleanFormat = getAnnotationBooleanFalse(annotationMap);
                    }
                    result = resolver.resolve(booleanFormat, valueMap);
                } else {
                    result = valueObject.toString();
                }
            } else if (value.isNull()) {
                result = "";
            } else {
                result = valueObject.toString();
            }

            if (valueMap != null) {
                String valueFormat = getAnnotationValueFormat(annotationMap);
                valueMap.put("value", result);
                valueMap.put(value.getFieldName(), result);
                result = resolver.resolve(valueFormat, valueMap);
            }
            return result;
        }

        public Enum getAnnotationObjectEnum(Map fieldAnnotation, Value value) {
            try {
                Enum objectEnum;
                String invokeGetterObjectEnum = (String) fieldAnnotation.get("invokeGetterObjectEnum");
                if (invokeGetterObjectEnum != null && invokeGetterObjectEnum.length() > 0) {
                    List<Object> dataList = flatMap(value.getData());
                    if (dataList.isEmpty()) {
                        objectEnum = null;
                    } else if (dataList.size() == 1) {
                        Object data = dataList.get(0);
                        Method invokeGetterObjectEnumMethod = data.getClass().getDeclaredMethod(invokeGetterObjectEnum);
                        objectEnum = (Enum) invokeGetterObjectEnumMethod.invoke(data);
                    } else {
                        Set<Enum> enumList = new LinkedHashSet<>();
                        for (Object data : dataList) {
                            Method invokeGetterObjectEnumMethod = data.getClass().getDeclaredMethod(invokeGetterObjectEnum);
                            Enum e = (Enum) invokeGetterObjectEnumMethod.invoke(data);
                            if (e != null) {
                                enumList.add(e);
                            }
                        }
                        if (enumList.isEmpty()) {
                            objectEnum = null;
                        } else if (enumList.size() == 1) {
                            objectEnum = enumList.iterator().next();
                        } else {
                            objectEnum = enumList.iterator().next();
                            log.warn("enumList = {}", enumList);
                        }
                    }
                } else {
                    objectEnum = (Enum) fieldAnnotation.get("objectEnum");
                }
                return objectEnum;
            } catch (Throwable t) {
                Util.sneakyThrows(t);
                return null;
            }
        }

        public String getAnnotationValueFormat(Map fieldAnnotation) {
            return (String) fieldAnnotation.get("valueFormat");
        }

        public String getAnnotationBooleanFalse(Map fieldAnnotation) {
            return (String) fieldAnnotation.get("booleanFalseFormat");
        }

        public String getAnnotationBooleanTrue(Map fieldAnnotation) {
            return (String) fieldAnnotation.get("booleanTrueFormat");
        }

        public String getAnnotationDateFormat(Map fieldAnnotation) {
            return (String) fieldAnnotation.get("dateFormat");
        }

        public String getAnnotationDelimiter(Map fieldAnnotation) {
            return (String) fieldAnnotation.get("delimiter");
        }

        public String getAnnotationValue(Map fieldAnnotation) {
            return (String) fieldAnnotation.get("value");
        }

        public String getPrefix(Map fieldAnnotation) {
            String template = getAnnotationValue(fieldAnnotation);
            IGPlaceholdersResolver resolver = getResolver();
            if (resolver.existResolve(template)) {
                return "";
            } else {
                return template;
            }
        }

        public String toString(String delimiter) {
            StringJoiner joiner = new StringJoiner(delimiter);
            String addDisplayText = getInsertDisplayText();
            String deleteDisplayText = getDeleteDisplayText();
            String editDisplayText = getUpdateDisplayText();
            if (addDisplayText.length() > 0) {
                joiner.add(addDisplayText);
            }
            if (deleteDisplayText.length() > 0) {
                joiner.add(deleteDisplayText);
            }
            if (editDisplayText.length() > 0) {
                joiner.add(editDisplayText);
            }
            return joiner.toString();
        }

        @Override
        public String toString() {
            return toString("\n");
        }

        @Override
        public int compareTo(ModifyHistory o) {
            return toString("\n").compareTo(o.toString("\n"));
        }
    }
}
