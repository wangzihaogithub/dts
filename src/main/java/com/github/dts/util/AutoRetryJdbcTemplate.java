package com.github.dts.util;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * query自动重试的 JdbcTemplate 子类
 * 继承自 JdbcTemplate，对 RecoverableDataAccessException 自动重试
 * 调用方无感知，可直接替换原生 JdbcTemplate
 */
public class AutoRetryJdbcTemplate extends JdbcTemplate {

    public static final int DEFAULT_MAX_RETRIES = 3;
    private final int maxRetries;

    // 构造器：兼容父类构造器 + 重试参数
    public AutoRetryJdbcTemplate() {
        this(DEFAULT_MAX_RETRIES);
    }

    public AutoRetryJdbcTemplate(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    // 如果你使用 DataSource 初始化
    public AutoRetryJdbcTemplate(javax.sql.DataSource dataSource) {
        super(dataSource);
        this.maxRetries = DEFAULT_MAX_RETRIES;
    }

    public AutoRetryJdbcTemplate(DataSource dataSource, boolean lazyInit) {
        super(dataSource, lazyInit);
        this.maxRetries = DEFAULT_MAX_RETRIES;
    }

    public AutoRetryJdbcTemplate(javax.sql.DataSource dataSource, int maxRetries) {
        super(dataSource);
        this.maxRetries = maxRetries;
    }

    // ========== 重写核心方法，添加重试逻辑 ==========

    @Override
    public <T> T query(PreparedStatementCreator psc, PreparedStatementSetter pss, ResultSetExtractor<T> rse) throws DataAccessException {
        return retryWithRecovery(() -> super.query(psc, pss, rse));
    }

    @Override
    public <T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse) throws DataAccessException {
        return retryWithRecovery(() -> super.query(psc, rse));
    }

    @Override
    public <T> T query(String sql, PreparedStatementSetter pss, ResultSetExtractor<T> rse) throws DataAccessException {
        return retryWithRecovery(() -> super.query(sql, pss, rse));
    }

    @Override
    public <T> T query(String sql, Object[] args, int[] argTypes, ResultSetExtractor<T> rse) throws DataAccessException {
        return retryWithRecovery(() -> super.query(sql, args, argTypes, rse));
    }

    @Override
    public <T> T query(String sql, Object[] args, ResultSetExtractor<T> rse) throws DataAccessException {
        return retryWithRecovery(() -> super.query(sql, args, rse));
    }

    @Override
    public <T> T query(String sql, ResultSetExtractor<T> rse, Object... args) throws DataAccessException {
        return retryWithRecovery(() -> super.query(sql, rse, args));
    }

    @Override
    public <T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper) throws DataAccessException {
        return retryWithRecovery(() -> super.query(psc, rowMapper));
    }

    @Override
    public <T> List<T> query(String sql, PreparedStatementSetter pss, RowMapper<T> rowMapper) throws DataAccessException {
        return retryWithRecovery(() -> super.query(sql, pss, rowMapper));
    }

    @Override
    public <T> List<T> query(String sql, Object[] args, int[] argTypes, RowMapper<T> rowMapper) throws DataAccessException {
        return retryWithRecovery(() -> super.query(sql, args, argTypes, rowMapper));
    }

    @Override
    public <T> List<T> query(String sql, Object[] args, RowMapper<T> rowMapper) throws DataAccessException {
        return retryWithRecovery(() -> super.query(sql, args, rowMapper));
    }

    @Override
    public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
        return retryWithRecovery(() -> super.query(sql, rowMapper, args));
    }

    @Override
    public <T> Stream<T> queryForStream(PreparedStatementCreator psc, PreparedStatementSetter pss, RowMapper<T> rowMapper) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForStream(psc, pss, rowMapper));
    }

    @Override
    public <T> Stream<T> queryForStream(PreparedStatementCreator psc, RowMapper<T> rowMapper) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForStream(psc, rowMapper));
    }

    @Override
    public <T> Stream<T> queryForStream(String sql, PreparedStatementSetter pss, RowMapper<T> rowMapper) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForStream(sql, pss, rowMapper));
    }

    @Override
    public <T> Stream<T> queryForStream(String sql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForStream(sql, rowMapper, args));
    }

    @Override
    public <T> T queryForObject(String sql, Object[] args, int[] argTypes, RowMapper<T> rowMapper) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForObject(sql, args, argTypes, rowMapper));
    }

    @Override
    public <T> T queryForObject(String sql, Object[] args, RowMapper<T> rowMapper) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForObject(sql, args, rowMapper));
    }

    @Override
    public <T> T queryForObject(String sql, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForObject(sql, rowMapper, args));
    }

    @Override
    public <T> T queryForObject(String sql, Object[] args, int[] argTypes, Class<T> requiredType) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForObject(sql, args, argTypes, requiredType));
    }

    @Override
    public <T> T queryForObject(String sql, Object[] args, Class<T> requiredType) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForObject(sql, args, requiredType));
    }

    @Override
    public <T> T queryForObject(String sql, Class<T> requiredType, Object... args) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForObject(sql, requiredType, args));
    }

    @Override
    public Map<String, Object> queryForMap(String sql, Object[] args, int[] argTypes) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForMap(sql, args, argTypes));
    }

    @Override
    public Map<String, Object> queryForMap(String sql, Object... args) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForMap(sql, args));
    }

    @Override
    public <T> List<T> queryForList(String sql, Object[] args, int[] argTypes, Class<T> elementType) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForList(sql, args, argTypes, elementType));
    }

    @Override
    public <T> List<T> queryForList(String sql, Object[] args, Class<T> elementType) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForList(sql, args, elementType));
    }

    @Override
    public <T> List<T> queryForList(String sql, Class<T> elementType, Object... args) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForList(sql, elementType, args));
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql, Object[] args, int[] argTypes) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForList(sql, args, argTypes));
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql, Object... args) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForList(sql, args));
    }

    @Override
    public SqlRowSet queryForRowSet(String sql, Object[] args, int[] argTypes) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForRowSet(sql, args, argTypes));
    }

    @Override
    public SqlRowSet queryForRowSet(String sql, Object... args) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForRowSet(sql, args));
    }

    @Override
    public <T> T query(String sql, ResultSetExtractor<T> rse) throws DataAccessException {
        return retryWithRecovery(() -> super.query(sql, rse));
    }

    @Override
    public <T> List<T> query(String sql, RowMapper<T> rowMapper) throws DataAccessException {
        return retryWithRecovery(() -> super.query(sql, rowMapper));
    }

    @Override
    public <T> Stream<T> queryForStream(String sql, RowMapper<T> rowMapper) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForStream(sql, rowMapper));
    }

    @Override
    public Map<String, Object> queryForMap(String sql) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForMap(sql));
    }

    @Override
    public <T> T queryForObject(String sql, RowMapper<T> rowMapper) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForObject(sql, rowMapper));
    }

    @Override
    public <T> T queryForObject(String sql, Class<T> requiredType) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForObject(sql, requiredType));
    }

    @Override
    public <T> List<T> queryForList(String sql, Class<T> elementType) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForList(sql, elementType));
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForList(sql));
    }

    @Override
    public SqlRowSet queryForRowSet(String sql) throws DataAccessException {
        return retryWithRecovery(() -> super.queryForRowSet(sql));
    }


    // ========== 通用重试逻辑 ==========

    private <T> T retryWithRecovery(Supplier<T> operation) {
        int attempts = 0;
        while (true) {
            try {
                return operation.get();
            } catch (RecoverableDataAccessException e) {
                attempts++;
                if (attempts > maxRetries) {
                    throw e;
//                    throw new org.springframework.dao.DataAccessResourceFailureException(
//                            "Max retries (" + maxRetries + ") exceeded for SQL operation", e);
                }
                Thread.yield();
            }
        }
    }

}