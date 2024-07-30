package com.github.dts.controller;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch7x.ES7xAdapter;
import com.github.dts.util.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 根据非自增ID的全量灌数据，可以继承这个Controller
 * <pre>
 * id	                        group_name	    name
 * 2024071120001536320040986	陕西煤业化工集团	海南德璟置业投资有限责任公司
 * 2024071120001540020040987	陕西煤业化工集团	西安重装渭南橡胶制品有限公司
 * 2024071120001546920040988	仁怀市建工集团	仁怀城投中资智慧城市运营有限公司
 * 2024071120001563920040989	苏州城市建设投资发展集团	苏州物资控股（集团）有限责任公司
 * </pre>
 * <pre>
 * curl "<a href="http://localhost:8080/es7x/myxxx/syncById?id=2024071120255559720056013,2024071118325561520000001">http://localhost:8080/es7x/myxxx/syncById?id=2024071120255559720056013,2024071118325561520000001</a>"
 * curl "<a href="http://localhost:8080/es7x/myxxx/syncAll">http://localhost:8080/es7x/myxxx/syncAll</a>"
 * curl "<a href="http://localhost:8080/es7x/myxxx/stop">http://localhost:8080/es7x/myxxx/stop</a>"
 * </pre>
 */
public abstract class AbstractEs7xETLStringController {
    private static final Logger log = LoggerFactory.getLogger(AbstractEs7xETLStringController.class);

    private final ExecutorService executorService = Util.newFixedThreadPool(1000, 5000L,
            getClass().getSimpleName(), true);
    @Autowired(required = false)
    protected StartupServer startupServer;
    @Autowired(required = false)
    protected AbstractMessageService messageService;
    private boolean stop = false;

    @RequestMapping("/syncAll")
    public String syncAll(
            @RequestParam(required = false, defaultValue = "0") String offsetStart,
            @RequestParam(required = false, defaultValue = "500") int offsetAdd,
            @RequestParam String esIndexName,
            @RequestParam(required = false, defaultValue = "true") boolean append,
            @RequestParam(required = false, defaultValue = "false") boolean onlyCurrentIndex,
            @RequestParam(required = false, defaultValue = "100") int joinUpdateSize,
            String[] onlyFieldName) {
        List<ES7xAdapter> adapterList = startupServer.getAdapter(ES7xAdapter.class);
        if (adapterList.isEmpty()) {
            return "empty";
        }

        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(StringUtils::isNotBlank).collect(Collectors.toSet());
        executorService.execute(() -> {
            for (ES7xAdapter adapter : adapterList) {
                Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
                for (ESSyncConfig config : configMap.values()) {
                    JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                    String catalog = CanalConfig.DatasourceConfig.getCatalog(config.getDataSourceKey());

                    String minId = offsetStart;
                    Date timestamp = new Timestamp(System.currentTimeMillis());
                    AtomicInteger dmlSize = new AtomicInteger(0);
                    try {
                        List<Dml> list;
                        do {
                            list = syncAll(jdbcTemplate, catalog, minId, offsetAdd, append, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet, adapter, config);
                            dmlSize.addAndGet(list.size());
                            if (log.isInfoEnabled()) {
                                log.info("syncAll dmlSize = {}, minOffset = {} ", dmlSize.intValue(), minId);
                            }
                            minId = getDmlListMaxId(list);
                            if (stop) {
                                break;
                            }
                        } while (minId != null);
                        sendDone(timestamp, dmlSize.intValue());
                    } catch (Exception e) {
                        sendError(e, minId, timestamp, dmlSize.get());
                        throw e;
                    }
                }
            }
        });
        return "1";
    }

    @RequestMapping("/syncById")
    public Object syncById(@RequestParam String[] id,
                           @RequestParam String esIndexName,
                           @RequestParam(required = false, defaultValue = "false") boolean onlyCurrentIndex,
                           String[] onlyFieldName) {
        List<ES7xAdapter> adapterList = startupServer.getAdapter(ES7xAdapter.class);
        if (adapterList.isEmpty()) {
            return "empty";
        }
        int count = 0;
        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(StringUtils::isNotBlank).collect(Collectors.toSet());
        for (ES7xAdapter adapter : adapterList) {
            Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
            for (ESSyncConfig config : configMap.values()) {
                JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                String catalog = CanalConfig.DatasourceConfig.getCatalog(config.getDataSourceKey());
                stop = false;

                try {
                    count += id.length;
                    syncById(jdbcTemplate, catalog, Arrays.asList(id), onlyCurrentIndex, onlyFieldNameSet, adapter, config);
                } finally {
                    log.info("all sync end.  total = {} ", count);
                }
            }
        }
        return count;
    }

    @RequestMapping("/deleteTrim")
    public Integer deleteTrim(@RequestParam(required = false, defaultValue = "500") int offsetAdd,
                              @RequestParam String esIndexName,
                              @RequestParam(required = false, defaultValue = "1000") int maxSendMessageDeleteIdSize) {
        List<ES7xAdapter> adapterList = startupServer.getAdapter(ES7xAdapter.class);
        if (adapterList.isEmpty()) {
            return 0;
        }

        executorService.execute(() -> {
            for (ES7xAdapter adapter : adapterList) {
                int hitListSize = 0;
                int deleteSize = 0;
                List<String> deleteIdList = new ArrayList<>();
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                try {
                    Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
                    for (ESSyncConfig config : configMap.values()) {
                        ESSyncConfig.ESMapping esMapping = config.getEsMapping();
                        JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                        String pk = esMapping.getPk();
                        String tableName = esMapping.getSchemaItem().getMainTable().getTableName();

                        ESTemplate esTemplate = adapter.getEsTemplate();

                        Object[] searchAfter = null;
                        do {
                            ESTemplate.ESSearchResponse searchResponse = esTemplate.searchAfter(esMapping, searchAfter, offsetAdd);
                            List<ESTemplate.Hit> hitList = searchResponse.getHitList();
                            if (hitList.isEmpty()) {
                                break;
                            }
                            hitListSize += hitList.size();
                            String ids = hitList.stream().map(ESTemplate.Hit::getId).collect(Collectors.joining("\",\""));
                            String sql = String.format("select %s from %s where %s in (\"%s\")", pk, tableName, pk, ids);
                            Set<String> dbIds = new HashSet<>(jdbcTemplate.queryForList(sql, String.class));
                            for (ESTemplate.Hit hit : hitList) {
                                String id = hit.getId();
                                if (!dbIds.contains(id)) {
                                    esTemplate.delete(esMapping, id, null, null);
                                    deleteSize++;
                                    if (deleteIdList.size() < maxSendMessageDeleteIdSize) {
                                        deleteIdList.add(id);
                                    }
                                }
                            }
                            esTemplate.commit();
                            searchAfter = searchResponse.getLastSortValues();
                        } while (true);
                        sendTrimDone(timestamp, hitListSize, deleteSize, deleteIdList);
                    }
                } catch (Exception e) {
                    sendTrimError(e, timestamp, hitListSize, deleteSize, deleteIdList);
                }
            }
        });
        return 1;
    }

    @RequestMapping("/stop")
    public boolean stop() {
        stop = true;
        return stop;
    }

    @RequestMapping("/discard")
    public List discard(@RequestParam String clientIdentity) throws InterruptedException {
        List list = new ArrayList();
        for (StartupServer.ThreadRef thread : startupServer.getCanalThread(clientIdentity)) {
            list.add(thread.getCanalThread().getConnector().setDiscard(true));
        }
        return list;
    }

    protected List<Dml> syncAll(JdbcTemplate jdbcTemplate, String catalog, String minId, int limit, boolean append, boolean onlyCurrentIndex,
                                int joinUpdateSize, Collection<String> onlyFieldNameSet,
                                ES7xAdapter esAdapter, ESSyncConfig config) {
        String tableName = config.getEsMapping().getSchemaItem().getMainTable().getTableName();
        String pk = config.getEsMapping().getPk();
        List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, minId, limit, pk, tableName, config);
        if (dmlList.isEmpty()) {
            return dmlList;
        }
        if (!append) {
            String dmlListMaxId = getDmlListMaxId(dmlList);
            ESBulkRequest.ESBulkResponse esBulkResponse = esAdapter.getEsTemplate().deleteByRange(config.getEsMapping(), ESSyncConfig.ES_ID_FIELD_NAME, minId, dmlListMaxId, limit);
        }
        esAdapter.sync(dmlList, false, true, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet);
        return dmlList;
    }

    protected int syncById(JdbcTemplate jdbcTemplate, String catalog, Collection<String> id, boolean onlyCurrentIndex, Collection<String> onlyFieldNameSet,
                           ES7xAdapter esAdapter, ESSyncConfig config) {
        if (id == null || id.isEmpty()) {
            return 0;
        }
        int count = 0;
        String tableName = config.getEsMapping().getSchemaItem().getMainTable().getTableName();
        String pk = config.getEsMapping().getPk();
        for (String i : id) {
            List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, i, 1, pk, tableName, config);
            for (Dml dml : dmlList) {
                dml.setDestination(esAdapter.getConfiguration().getCanalAdapter().getDestination());
            }
            esAdapter.sync(dmlList, false, true, onlyCurrentIndex, 1, onlyFieldNameSet);
            count += dmlList.size();
        }
        return count;
    }

    protected void sendDone(Date startTime, int dmlSize) {
        String title = "ES搜索全量刷数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getClass().getSimpleName()
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n DML条数 = " + dmlSize;
        messageService.send(title, content);
    }

    protected void sendTrimDone(Date startTime, int dmlSize, int deleteSize, List<String> deleteIdList) {
        String title = "ES搜索全量校验Trim数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getClass().getSimpleName()
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 删除条数 = " + deleteSize
                + ",\n\n 删除ID = " + deleteIdList;
        messageService.send(title, content);
    }

    private void sendTrimError(Throwable throwable, Date timestamp, int dmlSize, int deleteSize, List<String> deleteIdList) {
        String title = "ES搜索全量校验Trim数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getClass().getSimpleName()
                + ",\n\n 开始时间 = " + timestamp
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 删除条数 = " + deleteSize
                + ",\n\n 删除ID = " + deleteIdList
                + ",\n\n 异常 = " + throwable
                + ",\n\n 明细 = " + writer;
        messageService.send(title, content);
    }

    private void sendError(Throwable throwable, String minId, Date timestamp, int dmlSize) {
        String title = "ES搜索全量刷数据-异常";
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));

        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 对象 = " + getClass().getSimpleName()
                + ",\n\n minOffset = " + minId
                + ",\n\n 开始时间 = " + timestamp
                + ",\n\n DML条数 = " + dmlSize
                + ",\n\n 异常 = " + throwable
                + ",\n\n 明细 = " + writer;
        messageService.send(title, content);
    }

    private static String getDmlListMaxId(List<Dml> list) {
        if (list.isEmpty()) {
            return null;
        }
        Dml dml = list.get(list.size() - 1);
        List<String> pkNames = dml.getPkNames();
        if (pkNames.size() != 1) {
            throw new IllegalArgumentException("pkNames.size() != 1: " + pkNames);
        }
        String pkName = pkNames.iterator().next();
        Map<String, Object> last = dml.getData().get(dml.getData().size() - 1);
        Object o = last.get(pkName);
        return o == null || "".equals(o) ? null : o.toString();
    }

    protected List<Dml> convertDmlList(JdbcTemplate jdbcTemplate, String catalog, String minId, int limit, String tableName, String idColumnName, ESSyncConfig config) {
        List<Map<String, Object>> jobList = selectList(jdbcTemplate, minId, limit, tableName, idColumnName);
        List<Dml> dmlList = new ArrayList<>();
        for (Map<String, Object> row : jobList) {
            dmlList.addAll(Dml.convertInsert(Arrays.asList(row), Arrays.asList(idColumnName), tableName, catalog, new String[]{config.getDestination()}));
        }
        return dmlList;
    }

    protected List<Map<String, Object>> selectList(JdbcTemplate jdbcTemplate, String minId, int limit, String tableName, String idColumnName) {
        String sql;
        if (limit == 1) {
            sql = "select * from " + tableName + " where " + idColumnName + " = ? limit ?";
        } else {
            sql = "select * from " + tableName + " where " + idColumnName + " > ? limit ?";
        }
        return jdbcTemplate.queryForList(sql, minId, limit);
    }

}