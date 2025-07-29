package com.github.dts.controller;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch.etl.IntESETLService;
import com.github.dts.util.Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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

    @Autowired(required = false)
    public void setStartupServer(StartupServer startupServer) {
        this.intESETLService = new IntESETLService(getClass().getSimpleName(), startupServer);
    }

    @RequestMapping("/updateEsNestedDiff")
    public int updateEsNestedDiff(@RequestParam String esIndexName,
                                  @RequestParam(required = false, defaultValue = "6") int threads,
                                  @RequestParam(required = false, defaultValue = "1000") int offsetAdd,
                                  // 比较字段：必须是嵌套字段：空=全部，
                                  Long startId,
                                  Long endId,
                                  String[] diffFields,
                                  String[] adapterNames,
                                  @RequestParam(required = false, defaultValue = "50") int maxSendMessageSize) {
        return intESETLService.updateEsNestedDiff(esIndexName, startId, endId, offsetAdd, threads,
                diffFields == null ? null : new LinkedHashSet<>(Arrays.asList(diffFields)), maxSendMessageSize,
                adapterNames == null ? null : Arrays.asList(adapterNames)).size();
    }

    @RequestMapping("/updateEsDiff")
    public int updateEsDiff(@RequestParam String esIndexName,
                            @RequestParam(required = false, defaultValue = "6") int threads,
                            @RequestParam(required = false, defaultValue = "1000") int offsetAdd,
                            Long startId,
                            Long endId,
                            // 比较字段：不含嵌套字段：空=全部，
                            String[] diffFields,
                            String[] adapterNames,
                            @RequestParam(required = false, defaultValue = "50") int maxSendMessageSize) {
        return intESETLService.updateEsDiff(esIndexName, startId, endId, offsetAdd, threads,
                diffFields == null ? null : new LinkedHashSet<>(Arrays.asList(diffFields)), maxSendMessageSize,
                adapterNames == null ? null : Arrays.asList(adapterNames)).size();
    }

    @RequestMapping("/deleteEsTrim")
    public int deleteEsTrim(@RequestParam String esIndexName,
                            Long startId,
                            Long endId,
                            String[] adapterNames,
                            @RequestParam(required = false, defaultValue = "1000") int offsetAdd,
                            @RequestParam(required = false, defaultValue = "1000") int maxSendMessageDeleteIdSize) {
        return intESETLService.deleteEsTrim(esIndexName, startId, endId, offsetAdd, maxSendMessageDeleteIdSize,
                adapterNames == null ? null : Arrays.asList(adapterNames)).size();
    }

    @RequestMapping("/syncAll")
    public List<IntESETLService.SyncRunnable> syncAll(
            @RequestParam String esIndexName,
            @RequestParam(required = false, defaultValue = "50") int threads,
            @RequestParam(required = false, defaultValue = "0") long offsetStart,
            Long offsetEnd,
            @RequestParam(required = false, defaultValue = "500") int offsetAdd,
            @RequestParam(required = false, defaultValue = "true") boolean append,
            @RequestParam(required = false, defaultValue = "true") boolean onlyCurrentIndex,
            @RequestParam(required = false, defaultValue = "100") int joinUpdateSize,
            String[] onlyFieldName,
            String[] adapterNames,
            String sqlWhere,
            @RequestParam(required = false, defaultValue = "false") boolean insertIgnore) {
        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(Util::isNotBlank).collect(Collectors.toCollection(LinkedHashSet::new));
        return intESETLService.syncAll(esIndexName, threads, offsetStart, offsetEnd, offsetAdd, append, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet,
                adapterNames == null ? null : Arrays.asList(adapterNames), sqlWhere, insertIgnore);
    }

    @RequestMapping("/syncById")
    public Object syncById(@RequestParam Long[] id,
                           @RequestParam String esIndexName,
                           @RequestParam(required = false, defaultValue = "true") boolean onlyCurrentIndex,
                           String[] onlyFieldName,
                           String[] adapterNames) {
        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(Util::isNotBlank).collect(Collectors.toCollection(LinkedHashSet::new));
        return intESETLService.syncById(id, esIndexName, onlyCurrentIndex, onlyFieldNameSet,
                adapterNames == null ? null : Arrays.asList(adapterNames));
    }

    @RequestMapping("/stop")
    public boolean stop() {
        intESETLService.stopSync();
        return true;
    }

    @RequestMapping("/discard")
    public List discard(@RequestParam String clientIdentity) throws InterruptedException {
        return intESETLService.discard(clientIdentity);
    }

}