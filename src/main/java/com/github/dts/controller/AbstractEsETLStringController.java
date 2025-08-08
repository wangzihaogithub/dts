package com.github.dts.controller;

import com.github.dts.canal.StartupServer;
import com.github.dts.impl.elasticsearch.ESAdapter;
import com.github.dts.impl.elasticsearch.etl.StringEsETLService;
import com.github.dts.util.DefaultESTemplate;
import com.github.dts.util.EsActionResponse;
import com.github.dts.util.EsTaskCompletableFuture;
import com.github.dts.util.Util;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private StartupServer startupServer;

    @Autowired(required = false)
    public void setStartupServer(StartupServer startupServer) {
        this.startupServer = startupServer;
        this.stringEsETLService = new StringEsETLService(getClass().getSimpleName(), startupServer);
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
     * @return 任务ID
     */
    @RequestMapping("/reindex")
    public List<Map<String, Object>> reindex(@RequestParam(value = "esIndexName", required = true) String esIndexName,
                                             @RequestParam(value = "newIndexName", required = true) String newIndexName,
                                             @RequestParam(value = "adapterNames", required = false) String[] adapterNames,
                                             @RequestParam(value = "afterAliasRemoveAndAdd", required = false, defaultValue = "true") boolean afterAliasRemoveAndAdd,
                                             @RequestParam(value = "afterReindexCheckDiff", required = false, defaultValue = "true") boolean afterReindexCheckDiff,
                                             @RequestParam(value = "afterReindexCheckDiffOffsetAdd", required = false, defaultValue = "500") int afterReindexCheckDiffOffsetAdd) {
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
                        stringEsETLService.checkAll(esIndexName, Collections.singletonList(adapter.getName()), afterReindexCheckDiffOffsetAdd);
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
                                  @RequestParam(value = "offsetAdd", required = false, defaultValue = "500") int offsetAdd,
                                  @RequestParam(value = "startId", required = false) String startId,
                                  // 比较字段：必须是嵌套字段：空=全部，
                                  @RequestParam(value = "diffFields", required = false) String[] diffFields,
                                  @RequestParam(value = "adapterNames", required = false) String[] adapterNames,
                                  @RequestParam(value = "maxSendMessageSize", required = false, defaultValue = "50") int maxSendMessageSize) {
        return stringEsETLService.updateEsNestedDiff(esIndexName, startId, offsetAdd,
                diffFields == null ? null : new LinkedHashSet<>(Arrays.asList(diffFields)), maxSendMessageSize,
                adapterNames == null ? null : Arrays.asList(adapterNames)).size();
    }

    @RequestMapping("/updateEsDiff")
    public int updateEsDiff(@RequestParam(value = "esIndexName", required = true) String esIndexName,
                            @RequestParam(value = "offsetAdd", required = false, defaultValue = "1000") int offsetAdd,
                            // 比较字段：不含嵌套字段：空=全部，
                            @RequestParam(value = "diffFields", required = false) String[] diffFields,
                            @RequestParam(value = "adapterNames", required = false) String[] adapterNames,
                            @RequestParam(value = "maxSendMessageSize", required = false, defaultValue = "50") int maxSendMessageSize) {
        return stringEsETLService.updateEsDiff(esIndexName, offsetAdd,
                diffFields == null ? null : new LinkedHashSet<>(Arrays.asList(diffFields)), maxSendMessageSize,
                adapterNames == null ? null : Arrays.asList(adapterNames)).size();
    }

    @RequestMapping("/deleteEsTrim")
    public int deleteEsTrim(@RequestParam(value = "esIndexName", required = true) String esIndexName,
                            @RequestParam(value = "adapterNames", required = false) String[] adapterNames,
                            @RequestParam(value = "offsetAdd", required = false, defaultValue = "1000") int offsetAdd,
                            @RequestParam(value = "maxSendMessageDeleteIdSize", required = false, defaultValue = "1000") int maxSendMessageDeleteIdSize) {
        return stringEsETLService.deleteEsTrim(esIndexName, offsetAdd, maxSendMessageDeleteIdSize,
                adapterNames == null ? null : Arrays.asList(adapterNames)).size();
    }

    @RequestMapping("/syncAll")
    public Integer syncAll(
            @RequestParam(value = "esIndexName", required = true) String esIndexName,
            @RequestParam(value = "offsetStart", required = false, defaultValue = "0") String offsetStart,
            @RequestParam(value = "offsetAdd", required = false, defaultValue = "1000") int offsetAdd,
            @RequestParam(value = "onlyCurrentIndex", required = false, defaultValue = "true") boolean onlyCurrentIndex,
            @RequestParam(value = "joinUpdateSize", required = false, defaultValue = "100") int joinUpdateSize,
            @RequestParam(value = "onlyFieldName", required = false) String[] onlyFieldName,
            @RequestParam(value = "adapterNames", required = false) String[] adapterNames,
            @RequestParam(value = "sqlWhere", required = false) String sqlWhere,
            @RequestParam(value = "insertIgnore", required = false, defaultValue = "false") boolean insertIgnore) {
        Set<String> onlyFieldNameSet = onlyFieldName == null ? null : Arrays.stream(onlyFieldName).filter(Util::isNotBlank).collect(Collectors.toCollection(LinkedHashSet::new));
        return stringEsETLService.syncAll(esIndexName, offsetStart, offsetAdd, onlyCurrentIndex, joinUpdateSize, onlyFieldNameSet,
                adapterNames == null ? null : Arrays.asList(adapterNames), sqlWhere, insertIgnore).size();
    }

    @RequestMapping("/syncById")
    public Object syncById(@RequestParam(value = "id", required = true) String[] id,
                           @RequestParam(value = "esIndexName", required = true) String esIndexName,
                           @RequestParam(value = "onlyCurrentIndex", required = false, defaultValue = "true") boolean onlyCurrentIndex,
                           @RequestParam(value = "onlyFieldName", required = false) String[] onlyFieldName,
                           @RequestParam(value = "adapterNames", required = false) String[] adapterNames) {
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
    public List discard(@RequestParam(value = "clientIdentity", required = true) String clientIdentity) throws InterruptedException {
        return stringEsETLService.discard(clientIdentity);
    }

}