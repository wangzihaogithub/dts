package com.github.dts.util;

import com.github.dts.impl.elasticsearch7x.ES7xAdapter;

import java.util.List;
import java.util.Map;

/**
 * ES同步事件监听
 *
 * @author acer01
 */
public interface ESSyncServiceListener {

    void init(Map<String, ESSyncConfig> map);

    void onSyncAfter(List<Dml> dml, ES7xAdapter adapter, ESTemplate.BulkRequestList bulkRequestList);

}
