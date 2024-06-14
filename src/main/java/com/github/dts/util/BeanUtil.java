package com.github.dts.util;

import org.springframework.beans.BeanUtils;
import org.springframework.util.ReflectionUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Locale.ENGLISH;

/**
 * 数据转换(默认值,类型) + 属性操作 = BeanUtils
 * 属性操作 = PropertyUtils
 *
 * @author acer01
 */
public class BeanUtil {
    public static final IdentityHashMap EMPTY_IDENTITY_HASH_MAP = new IdentityHashMap() {
        @Override
        public Object put(Object key, Object value) {
            return null;
        }

        @Override
        public Object get(Object key) {
            return null;
        }
    };
    private static final Map<Class, Constructor> CONSTRUCTOR_NO_ARG_MAP = new LinkedHashMap<Class, Constructor>(128, 0.75F, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > 300;
        }
    };
    private static final Map<Class, Boolean> BASE_TYPE_FLAG_MAP = new LinkedHashMap<Class, Boolean>(256, 0.75F, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > 1000;
        }
    };
    private static final ThreadLocal<SimpleDateFormat> FORMAT_THREAD_LOCAL = ThreadLocal.withInitial(SimpleDateFormat::new);
    private static final Class[] EMPTY_CLASS_ARRAY = {};
    private static final Object[] EMPTY_OBJECT_ARRAY = {};
    private static final Method UNSAFE_ALLOCATE_INSTANCE_METHOD;
    private static final Object UNSAFE;

    static {
        Method unsafeAllocateInstanceMethod;
        Object unsafe;
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field f = unsafeClass.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = f.get(null);
            unsafeAllocateInstanceMethod = unsafeClass.getDeclaredMethod("allocateInstance", Class.class);
        } catch (Throwable e) {
            unsafe = null;
            unsafeAllocateInstanceMethod = null;
        }
        UNSAFE = unsafe;
        UNSAFE_ALLOCATE_INSTANCE_METHOD = unsafeAllocateInstanceMethod;
    }

    /**
     * 调用set方法
     *
     * @param fieldName  字段名称
     * @param fieldValue 字段值
     * @param target     对象实例
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     */
    public static boolean setFieldValue(String fieldName, Object fieldValue, Object target) throws IllegalAccessException, NoSuchFieldException {
        if (target == null) {
            return false;
        }
        Field field = ReflectionUtils.findField(target.getClass(), fieldName);
        Object castFieldValue;
        if (field != null) {
            castFieldValue = TypeUtil.cast(fieldValue, field.getType());
        } else {
            castFieldValue = fieldValue;
        }
        try {
            PropertyDescriptor descriptor = new PropertyDescriptor(fieldName, target.getClass(),
                    null,
                    "set" + fieldName.substring(0, 1).toUpperCase(ENGLISH) + fieldName.substring(1));
            Method writeMethod = descriptor.getWriteMethod();
            if (writeMethod != null) {
                writeMethod.invoke(target, castFieldValue);
                return true;
            }
        } catch (Exception e) {
            //skip
        }
        return setFieldValue(field, castFieldValue, target);
    }

    public static boolean setFieldValue(Field field, Object fieldValue, Object target) throws IllegalAccessException {
        if (field == null || target == null) {
            return false;
        }
        field.setAccessible(true);
        field.set(target, fieldValue);
        return true;
    }

    /**
     * 调用get方法
     *
     * @param fieldName 字段名称
     * @param target    对象实例
     * @return 字段值
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     */
    public static Object getFieldValue(String fieldName, Object target) throws IllegalAccessException, NoSuchFieldException {
        try {
            if (target == null) {
                return null;
            }
            PropertyDescriptor descriptor = new PropertyDescriptor(fieldName, target.getClass(),
                    "get" + fieldName.substring(0, 1).toUpperCase(ENGLISH) + fieldName.substring(1),
                    null);
            Method readMethod = descriptor.getReadMethod();
            if (readMethod != null) {
                return readMethod.invoke(target);
            }
        } catch (Exception e) {
            //skip
        }
        Field field = ReflectionUtils.findField(target.getClass(), fieldName);
        if (field == null) {
            throw new NoSuchFieldException("field=" + fieldName);
        }
        return getFieldValue(field, target);
    }

    public static Object getFieldValue(int fieldIndex, Object target) throws IllegalAccessException {
        if (target == null) {
            return null;
        }
        Class<?> targetClass = target.getClass();
        Field[] declaredFields = targetClass.getDeclaredFields();
        if (fieldIndex < 0 || declaredFields.length <= fieldIndex) {
            throw new IllegalAccessException("outof array length. declaredFieldLength=" + declaredFields.length + ", fieldIndex=" + fieldIndex + ",targetClass=" + targetClass);
        }
        return getFieldValue(declaredFields[fieldIndex], target);
    }

    public static Object getFieldValue(Field field, Object target) throws IllegalAccessException {
        if (target == null) {
            return null;
        }
        field.setAccessible(true);
        return field.get(target);
    }

    public static void copyProperties(Object source, Object dest) {
        BeanUtils.copyProperties(source, dest);
    }

    /**
     * 类型转换
     *
     * @param source
     * @param returnType
     * @param <T>
     * @param <R>
     * @return
     */
    public static <T, R> List<R> transform(Collection<T> source, Class<R> returnType) {
        if (source == null) {
            return (List<R>) source;
        }
        IdentityHashMap<Object, Object> context = new IdentityHashMap<>();
        return source.stream()
                .map(o -> transform(o, returnType, context))
                .collect(Collectors.toList());
    }

    public static <T, R> R[] transformArray(Collection<T> source, Class<R> returnType) {
        if (source == null) {
            return (R[]) null;
        }
        IdentityHashMap<Object, Object> context = new IdentityHashMap<>();
        return source.stream()
                .map(o -> transform(o, returnType, context))
                .toArray(value -> (R[]) Array.newInstance(returnType, value));
    }

    /**
     * 支持嵌套集合深拷贝,
     * class不同也支持拷贝
     *
     * @param source
     * @param <R>
     * @return
     */
    public static <R> R copy(R source) {
        return transform(source, null);
    }

    public static <R> R clone(R source) {
        if (source == null) {
            return null;
        }
        R clone = null;
        try {
            clone = (R) source.getClass().getMethod("clone").invoke(source);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            e.printStackTrace();
        }
        return clone;
    }

    public static boolean isBaseType(Class type) {
        if (type == null) {
            return true;
        }
        return BASE_TYPE_FLAG_MAP.computeIfAbsent(type, o -> {
            return o.isPrimitive()
                    || o.isEnum()
                    || o == Object.class
                    || o == Integer.class
                    || o == Byte.class
                    || o == Long.class
                    || o == Double.class
                    || o == Float.class
                    || o == Character.class
                    || o == Short.class
                    || o == Boolean.class
                    || Number.class.isAssignableFrom(o)
                    || CharSequence.class.isAssignableFrom(o)
                    || Date.class.isAssignableFrom(o)
                    || TemporalAccessor.class.isAssignableFrom(o)
                    ;
        });
    }

    public static boolean isBaseType(Object source) {
        if (source == null) {
            return true;
        }
        Class type = source instanceof Class ? (Class) source : source.getClass();
        return isBaseType(type);
    }

    public static <R> R deepCopyTo(Object source, R target) {
        Class targetType = target.getClass();
        if (isMap(targetType)) {
            return (R) transformBeanToMap(source, (Map) target, (Class<Map>) targetType, EMPTY_IDENTITY_HASH_MAP);
        } else {
            return transformBeanToBean(source, target, (Class<R>) targetType, EMPTY_IDENTITY_HASH_MAP);
        }
    }

    private static <R> R transformBean(Object source, Class<R> returnType, IdentityHashMap context) {
        if (isMap(returnType)) {
            return (R) transformBeanToMap(source, null, (Class<Map>) returnType, context);
        } else {
            return transformBeanToBean(source, null, returnType, context);
        }
    }

    private static <R extends Map> R transformBeanToMap(Object source, R target, Class<R> returnType, IdentityHashMap context) {
        if (target == null) {
            target = (R) context.get(source);
            if (target != null) {
                return target;
            }
        }
        if (target == null) {
            target = newInstanceMapOrCollection(returnType, returnType);
        }
        context.put(source, target);

        BeanMap sourceBeanMap = new BeanMap(source);
        BeanMap targetBeanMap = target instanceof BeanMap ? (BeanMap) target : null;
        for (Map.Entry<String, Object> entry : sourceBeanMap.entrySet()) {
            String key = entry.getKey();
            Object sourceValue = entry.getValue();
            Field sourceField = sourceBeanMap.getField(key);
            if(sourceField != null && Modifier.isTransient(sourceField.getModifiers())){
                continue;
            }
            Object targetValue = transform(sourceValue, null, context);
            if (targetBeanMap != null) {
                targetBeanMap.set(key, targetValue);
            } else {
                target.put(key, targetValue);
            }
        }
        return target;
    }

    private static <R> R transformBeanToBean(Object source, R target, Class<R> returnType, IdentityHashMap context) {
        if (target == null) {
            target = (R) context.get(source);
            if (target != null) {
                return target;
            }
        }
        if (target == null) {
            target = newInstance(returnType, source.getClass());
        }
        context.put(source, target);

        BeanMap sourceBeanMap = new BeanMap(source);
        BeanMap targetBeanMap = new BeanMap(target);

        Map<String, PropertyDescriptor> targetDescriptorMap = new HashMap<>(targetBeanMap.getDescriptorMap());
        for (Map.Entry<String, Object> entry : sourceBeanMap.entrySet()) {
            String key = entry.getKey();
            Object sourceValue = entry.getValue();
            Field sourceField = sourceBeanMap.getField(key);
            if(sourceField != null && Modifier.isTransient(sourceField.getModifiers())){
                continue;
            }
            PropertyDescriptor targetDescriptor = targetBeanMap.getPropertyDescriptor(key);
            if (targetDescriptor == null) {
                // 目标对象没有这个字段
                continue;
            }
            Method writeMethod = targetDescriptor.getWriteMethod();
            Field field = BeanMap.getField(targetDescriptor);
            // 不可写
            if (writeMethod == null && field == null) {
                continue;
            }

            Object targetValue = copyAndCast(sourceValue, writeMethod, field, context);
            targetBeanMap.set(key, targetValue);
            targetDescriptorMap.remove(key);
        }

        // 剩余的交集
        for (Map.Entry<String, PropertyDescriptor> entry : targetDescriptorMap.entrySet()) {
            String key = entry.getKey();
            // 源字段没有这个属性, 防止set一个null
            if (!sourceBeanMap.containsKey(key)) {
                continue;
            }
            PropertyDescriptor targetDescriptor = entry.getValue();
            Object sourceValue = sourceBeanMap.get(key);
            Method writeMethod = targetDescriptor.getWriteMethod();
            Field field = BeanMap.getField(targetDescriptor);
            // 不可写
            if (writeMethod == null && field == null) {
                continue;
            }
            Object targetValue = copyAndCast(sourceValue, writeMethod, field, context);
            targetBeanMap.set(key, targetValue);
        }
        return target;
    }

    private static <R> R transformBaseType(Object source, Class<R> returnType) {
        String format = "yyyy-MM-dd HH:mm:ss";
        if (returnType.isAssignableFrom(source.getClass())) {
            return (R) source;
        } else if (source instanceof Date) {
            if (returnType == String.class) {
                return (R) formatShowTime((Date) source, format);
            }
        } else if (source instanceof TemporalAccessor) {
            if (returnType == String.class) {
                return (R) LocalDateTime.from((TemporalAccessor) source).format(DateTimeFormatter.ofPattern(format));
            }
        }
        return TypeUtil.cast(source, returnType);
    }

    private static <R> R transformArray(Object source, Class<R> returnType, Class elementType, IdentityHashMap context) {
        Object result = context.get(source);
        if (result != null) {
            return (R) result;
        }

        if (elementType == null) {
            elementType = source.getClass().getComponentType();
        }
        if (returnType.isArray()) {
            if (isBaseType(elementType)) {
                return (R) source;
            } else {
                int size = Array.getLength(source);
                result = (elementType == Object.class) ? (R) new Object[size] : (R) Array.newInstance(elementType, size);
                context.put(source, result);
                for (int i = 0; i < size; i++) {
                    Object o = Array.get(source, i);
                    Array.set(result, i, transform(o, elementType, context));
                }
            }
        } else if (Collection.class.isAssignableFrom(returnType)) {
            Collection list = newInstanceMapOrCollection(returnType, returnType);
            result = list;
            context.put(source, result);
            for (int i = 0, size = list.size(); i < size; i++) {
                Object o = Array.get(source, i);
                if (o == null) {
                    list.add(null);
                } else {
                    list.add(transform(o, elementType, context));
                }
            }
        } else {
            return null;
        }
        return (R) result;
    }

    private static <R> R transformCollection(Collection source, Class<R> returnType, Class elementType, IdentityHashMap context) {
        Object result = context.get(source);
        if (result != null) {
            return (R) result;
        }

        if (Collection.class.isAssignableFrom(returnType)) {
            Collection list = newInstanceMapOrCollection(source.getClass(), returnType);
            result = list;
            context.put(source, result);
            for (Object o : source) {
                list.add(transform(o, elementType, context));
            }
        } else if (returnType.isArray()) {
            if (elementType == null) {
                elementType = returnType.getComponentType();
            }
            boolean primitive = elementType.isPrimitive();
            int size = source.size();
            R array = (elementType == Object[].class) ? (R) new Object[size] : (R) Array.newInstance(elementType, size);
            result = array;
            context.put(source, result);
            int i = 0;
            for (Object o : source) {
                if (primitive && o == null) {
                    continue;
                }
                Array.set(array, i++, transform(o, elementType, context));
            }
        } else {
            result = null;
        }
        return (R) result;
    }

    private static <R> R transformMap(Map source, Class<R> returnType, IdentityHashMap context) {
        Object result = context.get(source);
        if (result != null) {
            return (R) result;
        }

        Map map = null;
        BeanMap beanMap = null;
        if (returnType == String.class) {
            return (R) source.toString();
        } else if (isMap(returnType)) {
            map = newInstanceMapOrCollection(source.getClass(), returnType);
            result = map;
        } else {
            result = newInstance(returnType);
            beanMap = new BeanMap(result);
        }
        context.put(source, result);
        for (Map.Entry entry : (((Map<Object, Object>) source).entrySet())) {
            Object key = entry.getKey();
            Object sourceValue = entry.getValue();
            if (beanMap != null) {
                if (key instanceof String) {
                    PropertyDescriptor descriptor = beanMap.getPropertyDescriptor(key);
                    if (descriptor != null) {
                        Method writeMethod = descriptor.getWriteMethod();
                        Field field = BeanMap.getField(descriptor);
                        // 不可写
                        if (writeMethod == null && field == null) {
                            continue;
                        }
                        Object copyValue = copyAndCast(sourceValue, writeMethod, field, context);

                        beanMap.set((String) key, copyValue);
                    } else {
                        // 目标实体类没有这个字段
                    }
                } else {
                    // key不是string, 不支持转成bean
                }
            } else {
                Object copyKey = transform(key, null, context);
                Object copyValue = transform(sourceValue, null, context);
                map.put(copyKey, copyValue);
            }
        }
        return (R) result;
    }

    /**
     * 支持嵌套集合深拷贝,
     * class不同也支持拷贝
     * 支持循环依赖
     *
     * @param source
     * @param returnType
     * @param <R>
     * @return
     */
    public static <R> R transform(Object source, Class<R> returnType) {
        if (source == null) {
            return (R) source;
        }
        IdentityHashMap<Object, Object> context = new IdentityHashMap<>();
//        context = EMPTY_IDENTITY_HASH_MAP;
        R result = transform(source, returnType, context);
        return result;
    }

    public static <R> R transform(Object source, Class<R> returnType, IdentityHashMap context) {
        if (source == null) {
            return (R) source;
        }
        if (returnType == null || returnType == Object.class) {
            returnType = (Class<R>) source.getClass();
        }
        R result;
        if (isBaseType(source)) {
            result = transformBaseType(source, returnType);
        } else if (source instanceof Collection) {
            result = transformCollection((Collection) source, returnType, null, context);
        } else if (source instanceof Map) {
            result = transformMap((Map) source, returnType, context);
        } else if (source.getClass().isArray()) {
            result = transformArray(source, returnType, null, context);
        } else {
            result = transformBean(source, returnType, context);
        }
        return result;
    }

    private static boolean isMap(Class type) {
        if (type == BeanMap.class) {
            return true;
        }
        if (BeanMap.class.isAssignableFrom(type)) {
            return false;
        }
        return Map.class.isAssignableFrom(type);
    }

    private static Object copyAndCast(Object sourceValue, Method targetWriteMethod, Field targetField, IdentityHashMap context) {
        if (sourceValue == null) {
            return null;
        }
        Object targetValue;
        Class targetValueType;
        if (targetWriteMethod != null) {
            targetValueType = targetWriteMethod.getParameterTypes()[0];
        } else if (targetField != null) {
            targetValueType = targetField.getType();
        } else {
            // 不可写
            return null;
        }
        if (targetValueType == Object.class || Modifier.isAbstract(targetValueType.getModifiers())) {
            targetValueType = sourceValue.getClass();
        }
        if (sourceValue instanceof Collection) {
            Class genericType = getGenericType(targetField);
            targetValue = transformCollection((Collection) sourceValue, targetValueType, genericType, context);
        } else if (sourceValue instanceof Map) {
            targetValue = transformMap((Map) sourceValue, targetValueType, context);
        } else if (sourceValue.getClass().isArray()) {
            targetValue = transformArray(sourceValue, targetValueType, null, context);
        } else {
            targetValue = transform(sourceValue, targetValueType, context);
        }
        return targetValue;
    }

    private static String formatShowTime(Date deliverTime, String fmtStr) {
        SimpleDateFormat dateFormat = FORMAT_THREAD_LOCAL.get();
        String s = dateFormat.toPattern();
        if (!fmtStr.equals(s)) {
            dateFormat.applyPattern(fmtStr);
        }
        return dateFormat.format(deliverTime);
    }

    /**
     * 获取泛型
     *
     * @param field 字段
     * @return 泛型
     */
    public static Class<?> getGenericType(Field field) {
        if (field == null) {
            return null;
        }
        if (field.getType().isArray()) {
            return field.getType().getComponentType();
        }

        Type genericType = field.getGenericType();
        if (!(genericType instanceof ParameterizedType)) {
            return null;
        }

        Type[] actualTypeArguments = ((ParameterizedType) genericType).getActualTypeArguments();
        if (actualTypeArguments.length != 1) {
            return null;
        }

        Type actualType = actualTypeArguments[0];
        if (actualType instanceof WildcardType) {
            Type[] upperBounds = ((WildcardType) actualType).getUpperBounds();
            if (upperBounds.length > 0 && upperBounds[0] instanceof Class) {
                return (Class) upperBounds[0];
            }
        }

        if (actualType instanceof Class) {
            return (Class) actualType;
        }
        return null;
    }

    public static <T> T newInstanceMapOrCollection(Class<?> type, Class returnType) {
        Function<Class, Object> instance = (type1) -> {
            Object result;
            if (Collection.class.isAssignableFrom(type1)) {
                if (Set.class.isAssignableFrom(type1)) {
                    result = new LinkedHashSet();
                } else if (Queue.class.isAssignableFrom(type1)) {
                    result = new ArrayDeque();
                } else {
                    result = new ArrayList();
                }
            } else if (Map.class.isAssignableFrom(type1)) {
                result = new LinkedHashMap();
            } else {
                throw new IllegalStateException("no support type " + type1);
            }
            return result;
        };

        Object result = null;
        if (!Modifier.isAbstract(returnType.getModifiers())) {
            try {
                result = newInstance(returnType);
            } catch (Exception e) {
                //
            }
        }

        if (result == null) {
            if (Modifier.isAbstract(type.getModifiers())) {
                result = instance.apply(type);
            } else {
                try {
                    result = newInstance(type);
                } catch (Exception e) {
                    result = instance.apply(type);
                }
            }
        }
        return (T) result;
    }

    public static <T> T newInstance(Class<T> type, Class def) {
        if (Modifier.isAbstract(type.getModifiers()) && type.isAssignableFrom(def)) {
            return (T) newInstance(def);
        } else {
            return (T) newInstance(type);
        }
    }

    public static <T> T newInstance(Class<T> type) {
        Constructor constructor = CONSTRUCTOR_NO_ARG_MAP.computeIfAbsent(type, o -> {
            try {
                return o.getConstructor(EMPTY_CLASS_ARRAY);
            } catch (Exception e) {
                return null;
            }
        });

        if (constructor == null) {
            boolean isJavaPackage = Optional.ofNullable(type.getPackage()).map(Package::getName).map(o -> o.startsWith("java.")).orElse(true);
            if (UNSAFE != null && !Modifier.isAbstract(type.getModifiers()) && !isJavaPackage) {
                try {
                    return (T) UNSAFE_ALLOCATE_INSTANCE_METHOD.invoke(UNSAFE, type);
                } catch (Throwable ignored) {
                }
            }
            throw new IllegalStateException("Can not newInstance(). class=" + type);
        }

        constructor.setAccessible(true);
        try {
            return (T) constructor.newInstance(EMPTY_OBJECT_ARRAY);
        } catch (Exception e) {
            boolean isJavaPackage = Optional.ofNullable(type.getPackage()).map(Package::getName).map(o -> o.startsWith("java.")).orElse(true);
            if (UNSAFE != null && !Modifier.isAbstract(type.getModifiers()) && !isJavaPackage) {
                try {
                    return (T) UNSAFE_ALLOCATE_INSTANCE_METHOD.invoke(UNSAFE, type);
                } catch (Throwable ignored) {
                }
            }
            throw new IllegalStateException("Can not newInstance(). e=" + e + ",class=" + type + ",constructor=" + constructor, e);
        }
    }

    /**
     * 复制数据，会覆盖
     *
     * @param copySource
     * @param copyTarget
     * @param <T>
     */
    public static <T> void copyToIfModify(T copySource, T copyTarget) {
        copyToIfModify(copySource, copyTarget, false);
    }

    public static <T> void copyToIfModify(T copySource, T copyTarget, boolean trim) {
        if (copySource == null || copyTarget == null) {
            return;
        }
        BeanMap targetBean = new BeanMap(copyTarget);
        for (Map.Entry<String, Object> entry : new BeanMap(copySource).entrySet()) {
            Object value = entry.getValue();
            if (value != null) {
                targetBean.set(entry.getKey(), value);
            }
        }
        if (trim) {
            for (Map.Entry<String, Object> entry : targetBean.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof String) {
                    entry.setValue(((String) value).trim());
                }
            }
        }
    }

    /**
     * 填充数据，不会覆盖
     *
     * @param copySource
     * @param copyTarget
     * @param <T>
     */
    public static <T> void fillIfNull(T copySource, T copyTarget) {
        if (copySource == null || copyTarget == null) {
            return;
        }
        BeanMap sourceBean = new BeanMap(copySource);
        BeanMap targetBean = new BeanMap(copyTarget);
        for (Map.Entry<String, Object> entry : targetBean.entrySet()) {
            String key = entry.getKey();
            Object targetValue = entry.getValue();
            Object sourceValue = sourceBean.get(key);
            if (targetValue == null && sourceValue != null) {
                targetBean.set(key, sourceValue);
            }
        }
    }

    public static <T> void copyTo(T copySource, T copyTarget) {
        copyTo(copySource, copyTarget, false);
    }

    public static <T> void copyTo(T copySource, T copyTarget, boolean trim) {
        if (copySource == null || copyTarget == null) {
            return;
        }
        BeanMap targetBean = new BeanMap(copyTarget);
        for (Map.Entry<String, Object> entry : new BeanMap(copySource).entrySet()) {
            targetBean.set(entry.getKey(), entry.getValue());
        }
        if (trim) {
            for (Map.Entry<String, Object> entry : targetBean.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof String) {
                    entry.setValue(((String) value).trim());
                }
            }
        }
    }

    public static String[] getBeanPackages() {
        Set<String> list = new LinkedHashSet<>();
        int packPreLength = 2;
        String[] packagePaths = BeanUtil.class.getPackage().getName().split("[.]");
        if (packagePaths.length >= packPreLength) {
            list.add(String.join(".", Arrays.asList(packagePaths).subList(0, packPreLength)));
        }
        return list.toArray(new String[0]);
    }

    public static Collection wrapCollection(Object iterableOrArray) {
        if (iterableOrArray instanceof Collection) {
            return (Collection) iterableOrArray;
        }
        if (iterableOrArray.getClass().isArray()) {
            ArrayList<Object> objects = new ArrayList<>();
            int length = Array.getLength(iterableOrArray);
            for (int i = 0; i < length; i++) {
                Object o = Array.get(iterableOrArray, i);
                objects.add(o);
            }
            return objects;
        }
        List list = new ArrayList();
        if (iterableOrArray instanceof Iterable) {
            Iterator iterator = ((Iterable) iterableOrArray).iterator();
            while (iterator.hasNext()) {
                list.add(iterator.next());
            }
            return list;
        }
        throw new IllegalStateException();
    }

    public static Iterable wrapIterable(Object iterableOrArray) {
        if (iterableOrArray instanceof Iterable) {
            return (Iterable) iterableOrArray;
        }
        if (iterableOrArray.getClass().isArray()) {
            ArrayList<Object> objects = new ArrayList<>();
            int length = Array.getLength(iterableOrArray);
            for (int i = 0; i < length; i++) {
                Object o = Array.get(iterableOrArray, i);
                objects.add(o);
            }
            return objects;
        }
        throw new IllegalStateException();
    }

    public static boolean isIterableOrArray(Class type) {
        if (Iterable.class.isAssignableFrom(type)) {
            return true;
        }
        return type.isArray();
    }

    public static boolean isBeanOrMap(Class type) {
        return isBeanOrMap(type, getBeanPackages());
    }

    public static boolean isBeanOrMap(Class type, String[] beanPackages) {
        if (Map.class.isAssignableFrom(type)) {
            return true;
        }
        Package typePackage = type.getPackage();
        if (typePackage == null) {
            return false;
        }
        String name = typePackage.getName();
        for (String beanPackage : beanPackages) {
            if (name.startsWith(beanPackage)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 1. 解决循环引用
     * 2. 保留相同引用关系
     *
     * @param <K>
     * @param <V>
     */
    public static class IdentityHashMap<K, V> {
        private final HashMap<Integer, V> source = new HashMap<>();

        public IdentityHashMap() {
        }

        public int id(Object object) {
            return System.identityHashCode(object);
        }

        public V get(K key) {
            int id = id(key);
            return source.get(id);
        }

        public V put(K key, V value) {
            int id = id(key);
            return source.put(id, value);
        }
    }

}
