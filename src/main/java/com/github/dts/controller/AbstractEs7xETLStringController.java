package com.github.dts.controller;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch7x.etl.StringEs7xETLService;
import com.github.dts.util.Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
    private StringEs7xETLService stringEs7xETLService;

    @Autowired(required = false)
    public void setStartupServer(StartupServer startupServer) {
        this.stringEs7xETLService = new StringEs7xETLService(getClass().getSimpleName(), startupServer);
    }

    @RequestMapping("/updateEsNestedDiff")
    public int updateEsNestedDiff(@RequestParam String esIndexName,
                                  @RequestParam(required = false, defaultValue = "500") int offsetAdd,
                                  String startId,
                                  // 比较字段：必须是嵌套字段：空=全部，
                                  String[] diffFields,
                                  @RequestParam(required = false, defaultValue = "500") int maxSendMessageSize) {
        return stringEs7xETLService.updateEsNestedDiff(esIndexName, startId, offsetAdd,
                diffFields == null ? null : new LinkedHashSet<>(Arrays.asList(diffFields)), maxSendMessageSize);
    }

    @RequestMapping("/updateEsDiff")
    public int updateEsDiff(@RequestParam String esIndexName,
                            @RequestParam(required = false, defaultValue = "500") int offsetAdd,
                            // 比较字段：不含嵌套字段：空=全部，
                            String[] diffFields,
                            @RequestParam(required = false, defaultValue = "500") int maxSendMessageSize) {
        return stringEs7xETLService.updateEsDiff(esIndexName, offsetAdd,
                diffFields == null ? null : new LinkedHashSet<>(Arrays.asList(diffFields)), maxSendMessageSize);
    }

    @RequestMapping("/deleteEsTrim")
    public int deleteEsTrim(@RequestParam String esIndexName,
                            @RequestParam(required = false, defaultValue = "500") int offsetAdd,
                            @RequestParam(required = false, defaultValue = "1000") int maxSendMessageDeleteIdSize) {
        return stringEs7xETLService.deleteEsTrim(esIndexName, offsetAdd, maxSendMessageDeleteIdSize);
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
        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(Util::isNotBlank).collect(Collectors.toCollection(LinkedHashSet::new));
        return stringEs7xETLService.syncAll(esIndexName, offsetStart, offsetAdd,
                append, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet);
    }

    @RequestMapping("/syncById")
    public Object syncById(@RequestParam String[] id,
                           @RequestParam String esIndexName,
                           @RequestParam(required = false, defaultValue = "true") boolean onlyCurrentIndex,
                           String[] onlyFieldName) {
        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(Util::isNotBlank).collect(Collectors.toCollection(LinkedHashSet::new));
        return stringEs7xETLService.syncById(id, esIndexName, onlyCurrentIndex, onlyFieldNameSet);
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