package com.github.dts.controller;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch.ESAdapter;
import com.github.dts.util.DefaultESTemplate;
import com.github.dts.util.ESSyncConfig;
import com.github.dts.util.EsActionResponse;
import com.github.dts.util.EsTaskCompletableFuture;
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
public abstract class AbstractEsSyncController {
    private StartupServer startupServer;

    @Autowired(required = false)
    public void setStartupServer(StartupServer startupServer) {
        this.startupServer = startupServer;
    }

    /**
     * reindex
     *
     * @param esIndexName            dts中的索引名称
     * @param newIndexName           新建一个索引名称
     * @param adapterNames           哪个ES实例
     * @param afterAliasRemoveAndAdd reindex之后，是否需要直接关联上？ true=需要直接关联上
     * @param afterReindexCheckDiff  reindex之后，是否需要追平？ true=需要追平
     * @return 任务ID
     */
    @RequestMapping("/reindex")
    public List<Map<String, Object>> reindex(@RequestParam(value = "esIndexName", required = true) String esIndexName,
                                             @RequestParam(value = "newIndexName", required = true) String newIndexName,
                                             @RequestParam(value = "adapterNames", required = false) String[] adapterNames,
                                             @RequestParam(value = "afterAliasRemoveAndAdd", required = false, defaultValue = "true") boolean afterAliasRemoveAndAdd,
                                             @RequestParam(value = "afterReindexCheckDiff", required = false, defaultValue = "true") boolean afterReindexCheckDiff) {
        List<ESAdapter> adapterList;
        if (adapterNames == null || adapterNames.length == 0) {
            adapterList = startupServer.getAdapter(ESAdapter.class);
        } else {
            adapterList = Stream.of(adapterNames).map(e -> startupServer.getAdapter(e, ESAdapter.class)).collect(Collectors.toList());
        }
        List<Map<String, Object>> resultList = new ArrayList<>();
        for (ESAdapter adapter : adapterList) {
            List<ESSyncConfig> configList = adapter.selectConfigList((c, i) -> i.equals(esIndexName));
            if (configList.isEmpty()) {
                continue;
            }
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
                        for (ESSyncConfig config : configList) {
                            adapter.etlSyncAll(config);
                        }
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

    @RequestMapping("/status")
    public Map<String, Map<String, Object>> status() {
        Map<String, Map<String, Object>> statusMap = new LinkedHashMap<>();
        for (ESAdapter esAdapter : startupServer.getAdapter(ESAdapter.class)) {
            Timestamp lastBinlogTimestamp = esAdapter.getLastBinlogTimestamp();
            Map<String, Object> status = new LinkedHashMap<>();
            status.put("name", esAdapter.getName());
            status.put("clientIdentity", esAdapter.getClientIdentity());
            status.put("lastBinlogTimestamp", String.valueOf(lastBinlogTimestamp));
            status.put("nestedMainJoinTableStatus", esAdapter.getNestedMainJoinTableStatus());
            status.put("nestedSlaveTableStatus", esAdapter.getNestedSlaveTableStatus());

            statusMap.put(esAdapter.getName(), status);
        }
        return statusMap;
    }

    @RequestMapping("/etl/syncAll")
    public int syncAll(@RequestParam(value = "adapterNames", required = false) String[] adapterNames) {
        List<ESAdapter> adapterList;
        if (adapterNames != null && adapterNames.length > 0) {
            adapterList = Arrays.stream(adapterNames).map(e -> startupServer.getAdapter(e, ESAdapter.class)).collect(Collectors.toList());
        } else {
            adapterList = startupServer.getAdapter(ESAdapter.class);
        }
        for (ESAdapter esAdapter : adapterList) {
            esAdapter.etlSyncAll();
        }
        return adapterList.size();
    }

    @RequestMapping("/etl/syncById")
    public int syncById(@RequestParam(value = "id", required = true) String[] id,
                        @RequestParam(value = "adapterNames", required = false) String[] adapterNames) {
        List<ESAdapter> adapterList;
        if (adapterNames != null && adapterNames.length > 0) {
            adapterList = Arrays.stream(adapterNames).map(e -> startupServer.getAdapter(e, ESAdapter.class)).collect(Collectors.toList());
        } else {
            adapterList = startupServer.getAdapter(ESAdapter.class);
        }
        List<String> idList = Arrays.asList(id);
        for (ESAdapter esAdapter : adapterList) {
            esAdapter.etlSyncById(idList);
        }
        return adapterList.size();
    }

    @RequestMapping("/etl/stop")
    public int etlStop(@RequestParam(value = "adapterNames", required = false) String[] adapterNames) {
        List<ESAdapter> adapterList;
        if (adapterNames != null && adapterNames.length > 0) {
            adapterList = Arrays.stream(adapterNames).map(e -> startupServer.getAdapter(e, ESAdapter.class)).collect(Collectors.toList());
        } else {
            adapterList = startupServer.getAdapter(ESAdapter.class);
        }
        for (ESAdapter esAdapter : adapterList) {
            esAdapter.etlSyncStop();
        }
        return adapterList.size();
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
    @RequestMapping("/binlog/ignore")
    public Object binlogIgnore(@RequestParam(value = "duration", required = false, defaultValue = "PT30M") String duration,
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
    @RequestMapping("/binlog/ignoreStop")
    public Object binlogIgnoreStop(@RequestParam(value = "adapterNames", required = false) String[] adapterNames) {
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

}