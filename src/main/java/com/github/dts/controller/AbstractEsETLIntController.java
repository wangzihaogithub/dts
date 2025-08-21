package com.github.dts.controller;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch.ESAdapter;
import com.github.dts.impl.elasticsearch.etl.IntESETLService;
import com.github.dts.util.DefaultESTemplate;
import com.github.dts.util.EsActionResponse;
import com.github.dts.util.EsTaskCompletableFuture;
import com.github.dts.util.Util;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 根据自增ID的全量灌数据，可以继承这个Controller
 * <pre>
 * curl "<a href="http://localhost:8080/es/myxxx/syncById?id=1,2">http://localhost:8080/es/myxxx/syncById?id=1,2</a>"
 * curl "<a href="http://localhost:8080/es/myxxx/syncAll">http://localhost:8080/es/myxxx/syncAll</a>"
 * curl "<a href="http://localhost:8080/es/myxxx/stop">http://localhost:8080/es/myxxx/stop</a>"
 * </pre>
 */
public abstract class AbstractEsETLIntController {
    private IntESETLService intESETLService;
    private StartupServer startupServer;

    @Autowired(required = false)
    public void setStartupServer(StartupServer startupServer) {
        this.startupServer = startupServer;
        this.intESETLService = new IntESETLService(getClass().getSimpleName(), startupServer);
    }

    /**
     * reindex
     *
     * @param esIndexName                    dts中的索引名称
     * @param newIndexName                   新建一个索引名称
     * @param adapterNames                   哪个ES实例
     * @param afterAliasRemoveAndAdd         reindex之后，是否需要直接关联上？ true=需要直接关联上
     * @param afterReindexCheckDiff          reindex之后，是否需要追平？ true=需要追平
     * @param afterReindexCheckDiffOffsetAdd reindex之后，如果需要追平？ 以几条记录为步长进行追平校验
     * @param afterReindexCheckDiffThreads   reindex之后，如果需要追平，所需的线程数量
     * @return 任务ID
     */
    @RequestMapping("/reindex")
    public List<Map<String, Object>> reindex(@RequestParam(value = "esIndexName", required = true) String esIndexName,
                                             @RequestParam(value = "newIndexName", required = true) String newIndexName,
                                             @RequestParam(value = "adapterNames", required = false) String[] adapterNames,
                                             @RequestParam(value = "afterAliasRemoveAndAdd", required = false, defaultValue = "true") boolean afterAliasRemoveAndAdd,
                                             @RequestParam(value = "afterReindexCheckDiff", required = false, defaultValue = "true") boolean afterReindexCheckDiff,
                                             @RequestParam(value = "afterReindexCheckDiffOffsetAdd", required = false, defaultValue = "500") int afterReindexCheckDiffOffsetAdd,
                                             @RequestParam(value = "afterReindexCheckDiffThreads", required = false, defaultValue = "3") int afterReindexCheckDiffThreads) {
        List<ESAdapter> adapterList;
        if (adapterNames == null || adapterNames.length == 0) {
            adapterList = startupServer.getAdapter(ESAdapter.class);
        } else {
            adapterList = Stream.of(adapterNames).map(e -> startupServer.getAdapter(e, ESAdapter.class)).collect(Collectors.toList());
        }
        List<Map<String, Object>> resultList = new ArrayList<>();
        for (ESAdapter adapter : adapterList) {
            DefaultESTemplate esTemplate = adapter.getEsTemplate();
            try {
                if (!esTemplate.getConnection().indexExist(esIndexName)) {
                    continue;
                }
            } catch (IOException e) {
                LoggerFactory.getLogger(getClass()).warn("reindex indexExist {} fail {}", esIndexName, e.toString(), e);
            }
            EsTaskCompletableFuture<EsActionResponse> reindex = esTemplate.reindex(esIndexName, newIndexName, afterAliasRemoveAndAdd);
            // 是否追平
            if (afterAliasRemoveAndAdd && afterReindexCheckDiff) {
                reindex.thenAccept(esActionResponse -> {
                    if (esActionResponse.isSuccess()) {
                        intESETLService.checkAll(esIndexName, Collections.singletonList(adapter.getName()), afterReindexCheckDiffOffsetAdd, afterReindexCheckDiffThreads, null, null);
                    }
                });
            }
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("adapterName", adapter.getName());
            result.put("taskId", reindex.getTaskId());
            resultList.add(result);
        }
        return resultList;
    }

    @RequestMapping("/updateEsNestedDiff")
    public int updateEsNestedDiff(@RequestParam(value = "esIndexName", required = true) String esIndexName,
                                  @RequestParam(value = "threads", required = false, defaultValue = "6") int threads,
                                  @RequestParam(value = "offsetAdd", required = false, defaultValue = "1000") int offsetAdd,
                                  // 比较字段：必须是嵌套字段：空=全部，
                                  @RequestParam(value = "startId", required = false) Long startId,
                                  @RequestParam(value = "endId", required = false) Long endId,
                                  @RequestParam(value = "diffFields", required = false) String[] diffFields,
                                  @RequestParam(value = "adapterNames", required = false) String[] adapterNames,
                                  @RequestParam(value = "maxSendMessageSize", required = false, defaultValue = "50") int maxSendMessageSize,
                                  @RequestParam(value = "esQueryBodyJson", required = false, defaultValue = "") String esQueryBodyJson) {
        return intESETLService.updateEsNestedDiff(esIndexName, startId, endId, offsetAdd, threads,
                diffFields == null ? null : new LinkedHashSet<>(Arrays.asList(diffFields)), maxSendMessageSize,
                adapterNames == null ? null : Arrays.asList(adapterNames), esQueryBodyJson).size();
    }

    @RequestMapping("/updateEsDiff")
    public int updateEsDiff(@RequestParam(value = "esIndexName", required = true) String esIndexName,
                            @RequestParam(value = "threads", required = false, defaultValue = "6") int threads,
                            @RequestParam(value = "offsetAdd", required = false, defaultValue = "1000") int offsetAdd,
                            @RequestParam(value = "startId", required = false) Long startId,
                            @RequestParam(value = "endId", required = false) Long endId,
                            // 比较字段：不含嵌套字段：空=全部，
                            @RequestParam(value = "diffFields", required = false) String[] diffFields,
                            @RequestParam(value = "adapterNames", required = false) String[] adapterNames,
                            @RequestParam(value = "maxSendMessageSize", required = false, defaultValue = "50") int maxSendMessageSize,
                            @RequestParam(value = "esQueryBodyJson", required = false, defaultValue = "") String esQueryBodyJson) {
        return intESETLService.updateEsDiff(esIndexName, startId, endId, offsetAdd, threads,
                diffFields == null ? null : new LinkedHashSet<>(Arrays.asList(diffFields)), maxSendMessageSize,
                adapterNames == null ? null : Arrays.asList(adapterNames), esQueryBodyJson).size();
    }

    @RequestMapping("/deleteEsTrim")
    public int deleteEsTrim(@RequestParam(value = "esIndexName", required = true) String esIndexName,
                            @RequestParam(value = "startId", required = false) Long startId,
                            @RequestParam(value = "endId", required = false) Long endId,
                            @RequestParam(value = "adapterNames", required = false) String[] adapterNames,
                            @RequestParam(value = "offsetAdd", required = false, defaultValue = "1000") int offsetAdd,
                            @RequestParam(value = "maxSendMessageDeleteIdSize", required = false, defaultValue = "50") int maxSendMessageDeleteIdSize) {
        return intESETLService.deleteEsTrim(esIndexName, startId, endId, offsetAdd, maxSendMessageDeleteIdSize,
                adapterNames == null ? null : Arrays.asList(adapterNames)).size();
    }

    @RequestMapping("/syncAll")
    public List<String> syncAll(
            @RequestParam(value = "esIndexName", required = true) String esIndexName,
            @RequestParam(value = "threads", required = false, defaultValue = "50") int threads,
            @RequestParam(value = "offsetStart", required = false) Long offsetStart,
            @RequestParam(value = "offsetEnd", required = false) Long offsetEnd,
            @RequestParam(value = "offsetAdd", required = false, defaultValue = "500") int offsetAdd,
            @RequestParam(value = "onlyCurrentIndex", required = false, defaultValue = "true") boolean onlyCurrentIndex,
            @RequestParam(value = "joinUpdateSize", required = false, defaultValue = "100") int joinUpdateSize,
            @RequestParam(value = "onlyFieldName", required = false) String[] onlyFieldName,
            @RequestParam(value = "adapterNames", required = false) String[] adapterNames,
            @RequestParam(value = "sqlWhere", required = false) String sqlWhere,
            @RequestParam(value = "insertIgnore", required = false, defaultValue = "false") boolean insertIgnore,
            @RequestParam(value = "maxSendMessageSize", required = false, defaultValue = "50") int maxSendMessageSize) {
        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(Util::isNotBlank).collect(Collectors.toCollection(LinkedHashSet::new));
        return intESETLService.syncAll(esIndexName, threads, offsetStart, offsetEnd, offsetAdd, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet,
                        adapterNames == null ? null : Arrays.asList(adapterNames), sqlWhere, insertIgnore, maxSendMessageSize)
                .stream().map(IntESETLService.SyncRunnable::getRange).map(Util.Range::toString).collect(Collectors.toList());
    }

    @RequestMapping("/syncById")
    public int syncById(@RequestParam(value = "id", required = true) Long[] id,
                        @RequestParam(value = "esIndexName", required = true) String esIndexName,
                        @RequestParam(value = "onlyCurrentIndex", required = false, defaultValue = "true") boolean onlyCurrentIndex,
                        @RequestParam(value = "onlyFieldName", required = false) String[] onlyFieldName,
                        @RequestParam(value = "adapterNames", required = false) String[] adapterNames) {
        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(Util::isNotBlank).collect(Collectors.toCollection(LinkedHashSet::new));
        return intESETLService.syncById(id, esIndexName, onlyCurrentIndex, onlyFieldNameSet,
                adapterNames == null ? null : Arrays.asList(adapterNames));
    }

    @RequestMapping("/status")
    public Map<String, Map<String, Object>> status() {
        Map<String, Map<String, Object>> statusMap = new LinkedHashMap<>();
        for (ESAdapter esAdapter : startupServer.getAdapter(ESAdapter.class)) {
            String name = esAdapter.getName();
            long ignore = esAdapter.getIgnoreEndTimestamp();

            Timestamp lastBinlogTimestamp = esAdapter.getLastBinlogTimestamp();
            Map<String, Object> status = new LinkedHashMap<>();
            status.put("name", name);
            status.put("ignoreEndTime", ignore == 0 ? "0" : new Timestamp(ignore).toString());
            status.put("clientIdentity", esAdapter.getClientIdentity());
            status.put("lastBinlogTimestamp", String.valueOf(lastBinlogTimestamp));
            status.put("nestedMainJoinTableStatus", esAdapter.getNestedMainJoinTableStatus());
            status.put("nestedSlaveTableStatus", esAdapter.getNestedSlaveTableStatus());

            statusMap.put(name, status);
        }
        return statusMap;
    }

    /**
     * 忽略增量的处理
     * <p>
     * 持续时间（必填） Examples:
     * <pre>
     *    "PT20.345S" -- parses as "20.345 seconds"
     *    "PT15M"     -- parses as "15 minutes" (where a minute is 60 seconds)
     *    "PT10H"     -- parses as "10 hours" (where an hour is 3600 seconds)
     *    "P2D"       -- parses as "2 days" (where a day is 24 hours or 86400 seconds)
     *    "P2DT3H4M"  -- parses as "2 days, 3 hours and 4 minutes"
     *    "P-6H3M"    -- parses as "-6 hours and +3 minutes"
     *    "-P6H3M"    -- parses as "-6 hours and -3 minutes"
     *    "-P-6H+3M"  -- parses as "+6 hours and -3 minutes"
     * </pre>
     *
     * @param duration     持续时间（必填）
     * @param adapterNames 停哪个处理器
     * @return 持续时间
     */
    @RequestMapping("/ignore")
    public Map<String, Object> ignore(@RequestParam(value = "duration", required = false, defaultValue = "PT30M") String duration,
                                      @RequestParam(value = "adapterNames", required = false) String[] adapterNames) {
        Duration parseDuration = Duration.parse(duration);
        List<ESAdapter> adapterList;
        if (adapterNames != null && adapterNames.length > 0) {
            adapterList = Arrays.stream(adapterNames).map(e -> startupServer.getAdapter(e, ESAdapter.class)).collect(Collectors.toList());
        } else {
            adapterList = startupServer.getAdapter(ESAdapter.class);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (ESAdapter esAdapter : adapterList) {
            long ignore = esAdapter.ignore(parseDuration);
            Map<String, String> time = new LinkedHashMap<>();
            time.put("startTime", new Timestamp(System.currentTimeMillis()).toString());
            time.put("endTime", new Timestamp(ignore).toString());
            result.put(esAdapter.getName(), time);
        }
        return result;
    }

    /**
     * 停止忽略增量的处理
     *
     * @param adapterNames 恢复哪个处理器
     * @return 持续时间
     */
    @RequestMapping("/ignoreStop")
    public Map<String, Object> ignoreStop(@RequestParam(value = "adapterNames", required = false) String[] adapterNames) {
        List<ESAdapter> adapterList;
        if (adapterNames != null && adapterNames.length > 0) {
            adapterList = Arrays.stream(adapterNames).map(e -> startupServer.getAdapter(e, ESAdapter.class)).collect(Collectors.toList());
        } else {
            adapterList = startupServer.getAdapter(ESAdapter.class);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (ESAdapter esAdapter : adapterList) {
            Timestamp lastBinlogTimestamp = esAdapter.getLastBinlogTimestamp();
            long beforeIgnoreEndTime = esAdapter.ignoreStop();
            Map<String, String> time = new LinkedHashMap<>();
            time.put("beforeIgnoreEndTime", beforeIgnoreEndTime == 0 ? "0" : new Timestamp(beforeIgnoreEndTime).toString());
            time.put("lastBinlogTimestamp", String.valueOf(lastBinlogTimestamp));
            result.put(esAdapter.getName(), time);
        }
        return result;
    }

    @RequestMapping("/stop")
    public boolean stop() {
        intESETLService.stopSync();
        return true;
    }

    @RequestMapping("/discard")
    public List discard(@RequestParam(value = "clientIdentity", required = true) String clientIdentity) throws InterruptedException {
        return intESETLService.discard(clientIdentity);
    }

}