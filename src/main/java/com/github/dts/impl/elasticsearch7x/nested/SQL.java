package com.github.dts.impl.elasticsearch7x.nested;

import com.github.dts.util.ESSyncUtil;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * sql语句
 */
public class SQL {
    private static final ThreadLocal<Queue<Placeholder>> PLACEHOLDER_QUEUE_THREAD_LOCAL = ThreadLocal.withInitial(ArrayDeque::new);
    private static final String PLACEHOLDER_BEGIN = "#{";
    private static final String PLACEHOLDER_END = "}";
    private final String exprSql;
    private final Object[] args;
    private final Map<String, Object> argsMap;

    public SQL(String exprSql, Object[] args, Map<String, Object> argsMap) {
        this.exprSql = exprSql;
        this.args = args;
        this.argsMap = argsMap;
    }

    /**
     * 获取占位符
     *
     * @param str 表达式
     * @return 多个占位符
     */
    private static Queue<Placeholder> getPlaceholderQueue(String str) {
        int charAt = 0;
        Queue<Placeholder> keys = PLACEHOLDER_QUEUE_THREAD_LOCAL.get();
        keys.clear();
        while (true) {
            charAt = str.indexOf(PLACEHOLDER_BEGIN, charAt);
            if (charAt == -1) {
                return keys;
            }
            charAt = charAt + PLACEHOLDER_BEGIN.length();
            keys.add(new Placeholder(str, charAt, str.indexOf(PLACEHOLDER_END, charAt)));
        }
    }

    /**
     * 将sql表达式与参数 转换为JDBC所需的sql对象
     *
     * @param expressionsSql sql表达式
     * @param args           参数
     * @return JDBC所需的sql对象
     */
    public static SQL convertToSql(String expressionsSql, Map<String, Object> args) {
        Queue<Placeholder> placeholderQueue = getPlaceholderQueue(expressionsSql);

        List<Object> argsList = new ArrayList<>();
        StringBuilder sqlBuffer = new StringBuilder(expressionsSql);
        Placeholder placeholder;
        while ((placeholder = placeholderQueue.poll()) != null) {
            int offset = expressionsSql.length() - sqlBuffer.length();
            sqlBuffer.replace(
                    placeholder.getBeginIndex() - PLACEHOLDER_BEGIN.length() - offset,
                    placeholder.getEndIndex() + PLACEHOLDER_END.length() - offset,
                    "?");
            Object value = args.get(placeholder.getKey());
            argsList.add(value);
        }
        return new SQL(ESSyncUtil.stringCache(sqlBuffer.toString()), argsList.toArray(), args);
    }

    public Map<String, Object> getArgsMap() {
        return argsMap;
    }

    public String getExprSql() {
        return exprSql;
    }

    public Object[] getArgs() {
        return args;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SQL)) return false;
        SQL sql = (SQL) o;
        return Objects.equals(exprSql, sql.exprSql) && Objects.deepEquals(args, sql.args);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exprSql, Arrays.hashCode(args));
    }

    @Override
    public String toString() {
        try {
            String replace = exprSql.replace("?", "%s");
            List<String> argsList = new ArrayList<>(args.length);
            for (Object arg : args) {
                String value;
                if (arg instanceof String) {
                    value = "'" + arg + "'";
                } else if (arg instanceof Date) {
                    value = "'" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(arg) + "'";
                } else {
                    value = String.valueOf(arg);
                }
                argsList.add(value);
            }
            return String.format(replace, argsList.toArray());
        } catch (Exception e) {
            return exprSql;
        }
    }

    /**
     * 占位符
     */
    private static class Placeholder {
        private final String source;
        private final int beginIndex;
        private final int endIndex;

        Placeholder(String source, int beginIndex, int endIndex) {
            this.source = source;
            this.beginIndex = beginIndex;
            this.endIndex = endIndex;
        }

        int getBeginIndex() {
            return beginIndex;
        }

        int getEndIndex() {
            return endIndex;
        }

        public String getKey() {
            return source.substring(beginIndex, endIndex);
        }

        @Override
        public String toString() {
            return getKey();
        }
    }
}
