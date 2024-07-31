package com.github.dts.controller;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch7x.etl.StringEs7xETLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

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
    private StringEs7xETLService stringEs7xETLService;

    @Autowired(required = false)
    public void setStartupServer(StartupServer startupServer) {
        this.stringEs7xETLService = new StringEs7xETLService(getClass().getSimpleName(), startupServer);
    }

    @RequestMapping("/syncAll")
    public Integer syncAll(
            @RequestParam String esIndexName,
            @RequestParam(required = false, defaultValue = "0") String offsetStart,
            @RequestParam(required = false, defaultValue = "500") int offsetAdd,
            @RequestParam(required = false, defaultValue = "true") boolean append,
            @RequestParam(required = false, defaultValue = "true") boolean onlyCurrentIndex,
            @RequestParam(required = false, defaultValue = "100") int joinUpdateSize,
            String[] onlyFieldName) {
        stringEs7xETLService.syncAll(esIndexName, offsetStart, offsetAdd,
                append, onlyCurrentIndex, joinUpdateSize, onlyFieldName);
        return 1;
    }

    @RequestMapping("/syncById")
    public Object syncById(@RequestParam String[] id,
                           @RequestParam String esIndexName,
                           @RequestParam(required = false, defaultValue = "true") boolean onlyCurrentIndex,
                           String[] onlyFieldName) {
        return stringEs7xETLService.syncById(id, esIndexName, onlyCurrentIndex, onlyFieldName);
    }

    @RequestMapping("/deleteTrim")
    public Integer deleteTrim(@RequestParam String esIndexName,
                              @RequestParam(required = false, defaultValue = "500") int offsetAdd,
                              @RequestParam(required = false, defaultValue = "1000") int maxSendMessageDeleteIdSize) {
        return stringEs7xETLService.deleteTrim(esIndexName, offsetAdd, maxSendMessageDeleteIdSize);
    }

    @RequestMapping("/stop")
    public boolean stop() {
        stringEs7xETLService.stopSync();
        return true;
    }

    @RequestMapping("/discard")
    public List discard(@RequestParam String clientIdentity) throws InterruptedException {
        return stringEs7xETLService.discard(clientIdentity);
    }

}