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
 * 根据自增ID的全量灌数据，可以继承这个Controller
 * <pre>
 * curl "<a href="http://localhost:8080/es7x/myxxx/syncById?id=1,2">http://localhost:8080/es7x/myxxx/syncById?id=1,2</a>"
 * curl "<a href="http://localhost:8080/es7x/myxxx/syncAll">http://localhost:8080/es7x/myxxx/syncAll</a>"
 * curl "<a href="http://localhost:8080/es7x/myxxx/stop">http://localhost:8080/es7x/myxxx/stop</a>"
 * </pre>
 */
public abstract class AbstractEs7xETLIntController {
    private static final Logger log = LoggerFactory.getLogger(AbstractEs7xETLIntController.class);

    private final ExecutorService executorService = Util.newFixedThreadPool(1000, 5000L,
            getClass().getSimpleName(), true);
    @Autowired(required = false)
    protected StartupServer startupServer;
    @Autowired(required = false)
    protected AbstractMessageService messageService;
    private boolean stop = false;

    private static Integer getDmlListMaxId(List<Dml> list) {
        int maxId = Integer.MIN_VALUE;
        for (Dml dml : list) {
            List<String> pkNames = dml.getPkNames();
            if (pkNames.size() != 1) {
                throw new IllegalArgumentException("pkNames.size() != 1: " + pkNames);
            }
            String pkName = pkNames.iterator().next();
            for (Map<String, Object> row : dml.getData()) {
                Object o = row.get(pkName);
                if (o == null) {
                    continue;
                }
                int id;
                if (o instanceof Integer) {
                    id = (Integer) o;
                } else {
                    id = Integer.parseInt(o.toString());
                }
                if (id > maxId) {
                    maxId = id;
                }
            }
        }
        return maxId == Integer.MIN_VALUE ? null : maxId;
    }

    protected List<Dml> convertDmlList(JdbcTemplate jdbcTemplate, String catalog, Integer minId, int limit, String tableName, String idColumnName, ESSyncConfig config) {
        List<Map<String, Object>> jobList = selectList(jdbcTemplate, minId, limit, tableName, idColumnName);
        List<Dml> dmlList = new ArrayList<>();
        for (Map<String, Object> row : jobList) {
            dmlList.addAll(Dml.convertInsert(Arrays.asList(row), Arrays.asList(idColumnName), tableName, catalog, new String[]{config.getDestination()}));
        }
        return dmlList;
    }

    protected List<Map<String, Object>> selectList(JdbcTemplate jdbcTemplate, Integer minId, int limit, String tableName, String idColumnName) {
        String sql;
        if (limit == 1) {
            sql = "select * from " + tableName + " where " + idColumnName + " = ? limit ?";
        } else {
            sql = "select * from " + tableName + " where " + idColumnName + " > ? limit ?";
        }
        return jdbcTemplate.queryForList(sql, minId, limit);
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
                            String ids = hitList.stream().map(ESTemplate.Hit::getId).collect(Collectors.joining(","));
                            String sql = String.format("select %s from %s where %s in (%s)", pk, tableName, pk, ids);
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

    @RequestMapping("/syncAll")
    public List<SyncRunnable> syncAll(
            @RequestParam(required = false, defaultValue = "50") int threads,
            @RequestParam(required = false, defaultValue = "0") int offsetStart,
            Integer offsetEnd,
            @RequestParam(required = false, defaultValue = "500") int offsetAdd,
            @RequestParam String esIndexName,
            @RequestParam(required = false, defaultValue = "true") boolean append,
            @RequestParam(required = false, defaultValue = "false") boolean discard,
            @RequestParam(required = false, defaultValue = "false") boolean onlyCurrentIndex,
            @RequestParam(required = false, defaultValue = "100") int joinUpdateSize,
            String[] onlyFieldName) {
        List<ES7xAdapter> adapterList = startupServer.getAdapter(ES7xAdapter.class);
        if (adapterList.isEmpty()) {
            return new ArrayList<>();
        }

        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(StringUtils::isNotBlank).collect(Collectors.toSet());

        this.stop = false;

        List<SyncRunnable> runnableList = new ArrayList<>();
        for (ES7xAdapter adapter : adapterList) {
            String clientIdentity = adapter.getClientIdentity();
            if (discard) {
                new Thread(() -> {
                    try {
                        discard(clientIdentity);
                    } catch (InterruptedException e) {
                        log.info("discard {}", e, e);
                    }
                }).start();
            }
            setSuspendEs7x(true, clientIdentity);

            Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
            for (ESSyncConfig config : configMap.values()) {

                JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                String catalog = CanalConfig.DatasourceConfig.getCatalog(config.getDataSourceKey());
                String pk = config.getEsMapping().getPk();
                String tableName = config.getEsMapping().getSchemaItem().getMainTable().getTableName();

                Integer maxId = offsetEnd == null || offsetEnd == Integer.MAX_VALUE ?
                        selectMaxId(jdbcTemplate, pk, tableName) : offsetEnd;

                Date timestamp = new Timestamp(System.currentTimeMillis());
                AtomicInteger done = new AtomicInteger(0);
                AtomicInteger dmlSize = new AtomicInteger(0);
                for (int i = 0; i < threads; i++) {
                    runnableList.add(new SyncRunnable(getClass().getSimpleName(), messageService, i, offsetStart, maxId, threads) {
                        @Override
                        public int run0(int offset) {
                            if (stop) {
                                return Integer.MAX_VALUE;
                            }
                            List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, offset, offsetAdd, tableName, pk, config);

                            if (!append) {
                                Integer dmlListMaxId = getDmlListMaxId(dmlList);
                                adapter.getEsTemplate().deleteByRange(config.getEsMapping(), ESSyncConfig.ES_ID_FIELD_NAME, offset, dmlListMaxId, offsetAdd);
                            }
                            if (!dmlList.isEmpty()) {
                                adapter.sync(dmlList, false, true, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet);
                            }
                            dmlSize.addAndGet(dmlList.size());
                            if (log.isInfoEnabled()) {
                                log.info("syncAll dmlSize = {}, minOffset = {} ", dmlSize.intValue(), SyncRunnable.minOffset(runnableList));
                            }
                            Integer dmlListMaxId = getDmlListMaxId(dmlList);
                            return dmlListMaxId == null ? Integer.MAX_VALUE : dmlListMaxId;
                        }

                        @Override
                        public void done() {
                            if (done.incrementAndGet() == threads) {
                                if (log.isInfoEnabled()) {
                                    log.info("syncAll done {}", this);
                                }
                                setSuspendEs7x(false, clientIdentity);
                                sendDone(runnableList, timestamp, dmlSize.intValue());
                            }
                        }
                    });
                }
                for (SyncRunnable runnable : runnableList) {
                    executorService.execute(runnable);
                }
            }
        }
        return runnableList;
    }

    @RequestMapping("/syncById")
    public Object syncById(@RequestParam Integer[] id,
                           @RequestParam(required = false, defaultValue = "false") boolean onlyCurrentIndex,
                           @RequestParam String esIndexName,
                           String[] onlyFieldName) {
        List<ES7xAdapter> adapterList = startupServer.getAdapter(ES7xAdapter.class);
        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(StringUtils::isNotBlank).collect(Collectors.toSet());

        int count = 0;
        for (ES7xAdapter adapter : adapterList) {
            Map<String, ESSyncConfig> configMap = adapter.getEsSyncConfigByIndex(esIndexName);
            for (ESSyncConfig config : configMap.values()) {
                JdbcTemplate jdbcTemplate = ESSyncUtil.getJdbcTemplateByKey(config.getDataSourceKey());
                String catalog = CanalConfig.DatasourceConfig.getCatalog(config.getDataSourceKey());
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

    @RequestMapping("/stop")
    public boolean stop() {
        stop = true;
        return stop;
    }

    private List<Map> discard(String clientIdentity) throws InterruptedException {
        List<Map> list = new ArrayList<>();
        for (StartupServer.ThreadRef thread : startupServer.getCanalThread(clientIdentity)) {
            list.add(thread.getCanalThread().getConnector().setDiscard(true));
        }
        return list;
    }

    private void setSuspendEs7x(boolean suspend, String clientIdentity) {
        List<StartupServer.ThreadRef> canalThread = startupServer.getCanalThread(clientIdentity);
        for (StartupServer.ThreadRef thread : canalThread) {
            if (suspend) {
                thread.stopThread();
            } else {
                thread.startThread();
            }
        }
    }

    protected Integer selectMaxId(JdbcTemplate jdbcTemplate, String idFiled, String tableName) {
        return jdbcTemplate.queryForObject("select max(" + idFiled + ") from " + tableName, Integer.class);
    }

    protected int syncById(JdbcTemplate jdbcTemplate, String catalog, Collection<Integer> id,
                           boolean onlyCurrentIndex, Collection<String> onlyFieldNameSet,
                           ES7xAdapter esAdapter, ESSyncConfig config) {
        if (id == null || id.isEmpty()) {
            return 0;
        }
        int count = 0;
        String pk = config.getEsMapping().getPk();
        String tableName = config.getEsMapping().getSchemaItem().getMainTable().getTableName();
        for (Integer i : id) {
            List<Dml> dmlList = convertDmlList(jdbcTemplate, catalog, i, 1, pk, tableName, config);
            esAdapter.sync(dmlList, false, true, onlyCurrentIndex, 1, onlyFieldNameSet);
            count += dmlList.size();
        }
        return count;
    }

    protected void sendDone(List<SyncRunnable> runnableList, Date startTime, int dmlSize) {
        String title = "ES搜索全量刷数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n DML条数 = " + dmlSize
                + ",\n\n 对象 = " + getClass().getSimpleName()
                + ",\n\n 明细 = " + runnableList;
        messageService.send(title, content);
    }

    protected void sendTrimDone(Date startTime, int dmlSize, int deleteSize, List<String> deleteIdList) {
        String title = "ES搜索全量校验Trim数据-结束";
        String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                + " \n\n   ---  "
                + ",\n\n 开始时间 = " + startTime
                + ",\n\n 结束时间 = " + new Timestamp(System.currentTimeMillis())
                + ",\n\n 校验条数 = " + dmlSize
                + ",\n\n 删除条数 = " + deleteSize
                + ",\n\n 删除ID = " + deleteIdList
                + ",\n\n 对象 = " + getClass().getSimpleName();
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

    public static abstract class SyncRunnable implements Runnable {
        private static final List<SyncRunnable> RUNNABLE_LIST = Collections.synchronizedList(new ArrayList<>());
        protected final int threadIndex;
        private final int maxId;
        private final int threads;
        private final int offset;
        private final int endOffset;
        private final int offsetStart;
        private final AbstractMessageService messageService;
        private final String name;
        protected int cOffset;
        private boolean done;

        public SyncRunnable(String name, AbstractMessageService messageService, int threadIndex, int offsetStart, int maxId, int threads) {
            this.name = name;
            this.messageService = messageService;
            this.threadIndex = threadIndex;
            this.maxId = maxId;
            this.threads = threads;
            this.offsetStart = offsetStart;
            int allocation = ((maxId + 1 - offsetStart) / threads);
            this.offset = offsetStart + (threadIndex * allocation);
            this.endOffset = threadIndex + 1 == threads ? offset + allocation * 3 : offset + allocation;
            this.cOffset = offset;
            RUNNABLE_LIST.add(this);
        }

        public static Integer minOffset(List<SyncRunnable> list) {
            Integer min = null;
            for (SyncRunnable runnable : list) {
                if (!runnable.done) {
                    if (min == null) {
                        min = runnable.cOffset;
                    } else {
                        min = Math.min(min, runnable.cOffset);
                    }
                }
            }
            return min;
        }

        private void sendError(Throwable throwable) {
            String title = "ES搜索全量刷数据-异常";
            StringWriter writer = new StringWriter();
            throwable.printStackTrace(new PrintWriter(writer));

            Integer minOffset = minOffset(RUNNABLE_LIST);

            String content = "  时间 = " + new Timestamp(System.currentTimeMillis())
                    + " \n\n   ---  "
                    + ",\n\n 对象 = " + name
                    + ",\n\n threadIndex = " + threadIndex
                    + ",\n\n minOffset = " + minOffset
                    + ",\n\n currentOffset = " + cOffset
                    + ",\n\n maxId = " + maxId
                    + ",\n\n offset = " + offset
                    + ",\n\n offsetStart = " + offsetStart
                    + ",\n\n endOffset = " + endOffset
                    + ",\n\n threads = " + threads
                    + ",\n\n 异常 = " + throwable
                    + ",\n\n 明细 = " + writer;
            messageService.send(title, content);
        }

        public boolean isDone() {
            return done;
        }

        @Override
        public void run() {
            int i = 0;
            try {
                for (cOffset = offset, i = 0; cOffset < endOffset; i++) {
                    long ts = System.currentTimeMillis();
                    int cOffsetbefore = cOffset;
                    cOffset = run0(cOffset);
                    log.info("all sync threadIndex {}/{}, offset = {}-{}, i ={}, remain = {}, cost = {}ms, maxId = {}",
                            threadIndex, threads, cOffsetbefore, cOffset, i,
                            endOffset - cOffset, System.currentTimeMillis() - ts, maxId);
                    if (cOffset == Integer.MAX_VALUE) {
                        break;
                    }
                    if (cOffsetbefore == cOffset && cOffsetbefore == maxId) {
                        break;
                    }
                }
            } catch (Exception e) {
                sendError(e);
                throw e;
            } finally {
                done = true;
                log.info("all sync done threadIndex {}/{}, offset = {}, i ={}, maxId = {}, info ={} ",
                        threadIndex, threads, cOffset, i, maxId, this);
                done();
            }
        }

        public void done() {

        }

        public abstract int run0(int offset);

        public int getThreadIndex() {
            return threadIndex;
        }

        public int getMaxId() {
            return maxId;
        }

        public int getThreads() {
            return threads;
        }

        public int getOffset() {
            return offset;
        }

        public int getEndOffset() {
            return endOffset;
        }

        public int getOffsetStart() {
            return offsetStart;
        }

        @Override
        public String toString() {
            return "SyncRunnable{" +
                    "threadIndex=" + threadIndex +
                    ", maxId=" + maxId +
                    ", threads=" + threads +
                    ", cOffset=" + cOffset +
                    ", offset=" + offset +
                    ", endOffset=" + endOffset +
                    ", offsetStart=" + offsetStart +
                    '}';
        }
    }
}