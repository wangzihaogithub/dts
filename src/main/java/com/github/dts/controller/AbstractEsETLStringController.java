package com.github.dts.controller;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch.etl.StringEsETLService;
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
 * curl "<a href="http://localhost:8080/es/myxxx/syncById?id=2024071120255559720056013,2024071118325561520000001">http://localhost:8080/es/myxxx/syncById?id=2024071120255559720056013,2024071118325561520000001</a>"
 * curl "<a href="http://localhost:8080/es/myxxx/syncAll">http://localhost:8080/es/myxxx/syncAll</a>"
 * curl "<a href="http://localhost:8080/es/myxxx/stop">http://localhost:8080/es/myxxx/stop</a>"
 * </pre>
 */
public abstract class AbstractEsETLStringController {
    private StringEsETLService stringEsETLService;

    @Autowired(required = false)
    public void setStartupServer(StartupServer startupServer) {
        this.stringEsETLService = new StringEsETLService(getClass().getSimpleName(), startupServer);
    }

    @RequestMapping("/updateEsNestedDiff")
    public int updateEsNestedDiff(@RequestParam String esIndexName,
                                  @RequestParam(required = false, defaultValue = "500") int offsetAdd,
                                  String startId,
                                  // 比较字段：必须是嵌套字段：空=全部，
                                  String[] diffFields,
                                  String[] adapterNames,
                                  @RequestParam(required = false, defaultValue = "500") int maxSendMessageSize) {
        return stringEsETLService.updateEsNestedDiff(esIndexName, startId, offsetAdd,
                diffFields == null ? null : new LinkedHashSet<>(Arrays.asList(diffFields)), maxSendMessageSize,
                adapterNames == null ? null : Arrays.asList(adapterNames));
    }

    @RequestMapping("/updateEsDiff")
    public int updateEsDiff(@RequestParam String esIndexName,
                            @RequestParam(required = false, defaultValue = "1000") int offsetAdd,
                            // 比较字段：不含嵌套字段：空=全部，
                            String[] diffFields,
                            String[] adapterNames,
                            @RequestParam(required = false, defaultValue = "500") int maxSendMessageSize) {
        return stringEsETLService.updateEsDiff(esIndexName, offsetAdd,
                diffFields == null ? null : new LinkedHashSet<>(Arrays.asList(diffFields)), maxSendMessageSize,
                adapterNames == null ? null : Arrays.asList(adapterNames));
    }

    @RequestMapping("/deleteEsTrim")
    public int deleteEsTrim(@RequestParam String esIndexName,
                            String[] adapterNames,
                            @RequestParam(required = false, defaultValue = "1000") int offsetAdd,
                            @RequestParam(required = false, defaultValue = "1000") int maxSendMessageDeleteIdSize) {
        return stringEsETLService.deleteEsTrim(esIndexName, offsetAdd, maxSendMessageDeleteIdSize,
                adapterNames == null ? null : Arrays.asList(adapterNames));
    }

    @RequestMapping("/syncAll")
    public Integer syncAll(
            @RequestParam String esIndexName,
            @RequestParam(required = false, defaultValue = "0") String offsetStart,
            @RequestParam(required = false, defaultValue = "1000") int offsetAdd,
            @RequestParam(required = false, defaultValue = "true") boolean append,
            @RequestParam(required = false, defaultValue = "true") boolean onlyCurrentIndex,
            @RequestParam(required = false, defaultValue = "100") int joinUpdateSize,
            String[] onlyFieldName,
            String[] adapterNames) {
        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(Util::isNotBlank).collect(Collectors.toCollection(LinkedHashSet::new));
        return stringEsETLService.syncAll(esIndexName, offsetStart, offsetAdd,
                append, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet,
                adapterNames == null ? null : Arrays.asList(adapterNames));
    }

    @RequestMapping("/syncById")
    public Object syncById(@RequestParam String[] id,
                           @RequestParam String esIndexName,
                           @RequestParam(required = false, defaultValue = "true") boolean onlyCurrentIndex,
                           String[] onlyFieldName,
                           String[] adapterNames) {
        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(Util::isNotBlank).collect(Collectors.toCollection(LinkedHashSet::new));
        return stringEsETLService.syncById(id, esIndexName, onlyCurrentIndex, onlyFieldNameSet,
                adapterNames == null ? null : Arrays.asList(adapterNames));
    }

    @RequestMapping("/stop")
    public boolean stop() {
        stringEsETLService.stopSync();
        return true;
    }

    @RequestMapping("/discard")
    public List discard(@RequestParam String clientIdentity) throws InterruptedException {
        return stringEsETLService.discard(clientIdentity);
    }

}