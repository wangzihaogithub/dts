package com.github.dts.controller;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch7x.etl.IntES7xETLService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
    private IntES7xETLService intES7xETLService;

    @Autowired(required = false)
    public void setStartupServer(StartupServer startupServer) {
        this.intES7xETLService = new IntES7xETLService(getClass().getSimpleName(), startupServer);
    }

    @RequestMapping("/deleteTrim")
    public int deleteTrim(@RequestParam String esIndexName,
                              @RequestParam(required = false, defaultValue = "500") int offsetAdd,
                              @RequestParam(required = false, defaultValue = "1000") int maxSendMessageDeleteIdSize) {
        return intES7xETLService.deleteTrim(esIndexName, offsetAdd, maxSendMessageDeleteIdSize);
    }

    @RequestMapping("/syncAll")
    public List<IntES7xETLService.SyncRunnable> syncAll(
            @RequestParam String esIndexName,
            @RequestParam(required = false, defaultValue = "50") int threads,
            @RequestParam(required = false, defaultValue = "0") int offsetStart,
            Integer offsetEnd,
            @RequestParam(required = false, defaultValue = "500") int offsetAdd,
            @RequestParam(required = false, defaultValue = "true") boolean append,
            @RequestParam(required = false, defaultValue = "false") boolean discard,
            @RequestParam(required = false, defaultValue = "true") boolean onlyCurrentIndex,
            @RequestParam(required = false, defaultValue = "100") int joinUpdateSize,
            String[] onlyFieldName) {
        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(StringUtils::isNotBlank).collect(Collectors.toCollection(LinkedHashSet::new));
        return intES7xETLService.syncAll(esIndexName, threads, offsetStart, offsetEnd, offsetAdd, append, discard, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet);
    }

    @RequestMapping("/syncById")
    public Object syncById(@RequestParam Integer[] id,
                           @RequestParam String esIndexName,
                           @RequestParam(required = false, defaultValue = "true") boolean onlyCurrentIndex,
                           String[] onlyFieldName) {
        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(StringUtils::isNotBlank).collect(Collectors.toCollection(LinkedHashSet::new));
        return intES7xETLService.syncById(id, esIndexName, onlyCurrentIndex, onlyFieldNameSet);
    }

    @RequestMapping("/stop")
    public boolean stop() {
        intES7xETLService.stopSync();
        return true;
    }

    @RequestMapping("/discard")
    public List discard(@RequestParam String clientIdentity) throws InterruptedException {
        return intES7xETLService.discard(clientIdentity);
    }

}