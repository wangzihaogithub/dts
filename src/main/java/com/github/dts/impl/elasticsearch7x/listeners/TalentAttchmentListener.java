package com.github.dts.impl.elasticsearch7x.listeners;//package com.github.dts.listeners;
//
//import com.alibaba.fastjson.util.TypeUtils;
//import com.alibaba.otter.canal.client.adapter.OuterAdapter;
//import com.alibaba.otter.canal.client.adapter.es.service.TextExtractService;
//import com.alibaba.otter.canal.client.adapter.es.service.TextExtractService.OutOfStringLengthException;
//import com.alibaba.otter.canal.client.adapter.support.Dml;
//import org.elasticsearch.script.Script;
//import org.elasticsearch.script.ScriptType;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.core.annotation.Order;
//import org.springframework.jdbc.core.JdbcTemplate;
//import org.springframework.stereotype.Component;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//
///**
// * talent_ext 表监听
// * 人才附件  (index=修改的是人才索引, listener=监听的是人才附件表)
// * <p>
// * 维护ES字段是
// * [
// * talent.attachment
// * ]
// *
// * @author hao 2019年11月5日 16:34:44
// */
//@Component
//@Order(10)
//public class TalentAttchmentListener extends AbstractESSyncServiceListener {
//    private static final String RESUME_URL_FIELD_NAME = "resume_attchment_url";
//    private static final String REPORT_URL_FIELD_NAME = "report_attchment_url";
//    @Autowired
//    private TextExtractService textExtractService;
//
//    public TalentAttchmentListener() {
//        super(Arrays.asList("talent_ext"));
//    }
//
//    @Override
//    public boolean match(Dml dml) {
//        switch (dml.getType()) {
//            case "UPDATE": {
//                for (Map<String, Object> old : dml.getOld()) {
//                    if (old.containsKey("resume_attachment_ids")
//                            || old.containsKey("report_attachment_ids")) {
//                        return true;
//                    }
//                }
//                return false;
//            }
//            case "DELETE":
//            case "INSERT": {
//                return true;
//            }
//            default: {
//                return false;
//            }
//        }
//    }
//
//    /**
//     * 同步之后要去做的事情
//     */
//    @Override
//    public void onSyncAfter(Dml dml, OuterAdapter adapter) {
//        switch (dml.getType()) {
//            case "INSERT": {
//                for (Map<String, Object> old : dml.getData()) {
//                    if (old.containsKey("resume_attachment_ids")) {
//                        insertResumeAttachment(dml, adapter);
//                    }
//                    if (old.containsKey("report_attachment_ids")) {
//                        insertReportAttachment(dml, adapter);
//                    }
//                }
//                break;
//            }
//            case "DELETE":
//            case "UPDATE": {
//                for (Map<String, Object> old : dml.getOld()) {
//                    if (old.containsKey("resume_attachment_ids")) {
//                        updateResumeAttachment(dml, adapter);
//                    }
//                    if (old.containsKey("report_attachment_ids")) {
//                        updateReportAttachment(dml, adapter);
//                    }
//                }
//                break;
//            }
//            default: {
//                //
//            }
//        }
//    }
//
//    private void updateResumeAttachment(Dml dml, OuterAdapter adapter) {
//        String destination = dml.getDestination();
//        JdbcTemplate jdbcTemplate = getJdbcTemplateByDefaultDS();
//        for (Map<String, Object> data : dml.getData()) {
//            Integer talentId = TypeUtils.castToInt(data.get("talent_id"));
//            if (talentId == null) {
//                continue;
//            }
//            List<String> attachment = textExtractService.selectAttachmentTextList(jdbcTemplate, talentId);
//            super.updateESByPrimaryKey(destination, "talent", talentId,
//                    Collections.singletonMap("resumeAttachment", attachment), adapter);
//        }
//    }
//
//    private void updateReportAttachment(Dml dml, OuterAdapter adapter) {
//        String destination = dml.getDestination();
//        JdbcTemplate jdbcTemplate = getJdbcTemplateByDefaultDS();
//        for (Map<String, Object> data : dml.getData()) {
//            Integer talentId = TypeUtils.castToInt(data.get("talent_id"));
//            if (talentId == null) {
//                continue;
//            }
//            List<String> attachment = textExtractService.selectReportAttachmentTextList(jdbcTemplate, talentId);
//            super.updateESByPrimaryKey(destination, "talent", talentId,
//                    Collections.singletonMap("reportAttachment", attachment), adapter);
//        }
//    }
//
//    private void insertResumeAttachment(Dml dml, OuterAdapter adapter) {
//        String destination = dml.getDestination();
//        for (Map<String, Object> data : dml.getData()) {
//            String url = (String) data.get(REPORT_URL_FIELD_NAME);
//            Number talentId = (Number) data.get("talent_id");
//            try {
//                //不控制字符长度,无限大
//                String extractText = textExtractService.extractToString(url, -1);
//                String idOrCode = "ctx._source.resumeAttachment = new ArrayList(); " +
//                        "ctx._source.resumeAttachment.add(params.extractText);";
//                Script script = new Script(ScriptType.INLINE, "painless",
//                        "ctx._source.resumeAttachment = new ArrayList(); " +
//                                "ctx._source.resumeAttachment.add(params.extractText);",
//                        Collections.singletonMap("extractText", extractText)
//                );
//                super.updateByScript(destination, idOrCode, ScriptType.INLINE.getId(), talentId,
//                        "painless", Collections.singletonMap("extractText", extractText), "talent", adapter);
//            } catch (IOException | OutOfStringLengthException e) {
//                logger.warn("talent.resumeAttachment error. table={},talentId={},url={},error={}", getListenerTable(), talentId, url, e.toString());
//            }
//        }
//    }
//
//    private void insertReportAttachment(Dml dml, OuterAdapter adapter) {
//        String destination = dml.getDestination();
//        for (Map<String, Object> data : dml.getData()) {
//            String url = (String) data.get(RESUME_URL_FIELD_NAME);
//            Number talentId = (Number) data.get("talent_id");
//            try {
//                //不控制字符长度,无限大
//                String extractText = textExtractService.extractToString(url, -1);
//                String idOrCode = "ctx._source.reportAttachment = new ArrayList(); " +
//                        "ctx._source.reportAttachment.add(params.extractText);";
//                super.updateByScript(destination, idOrCode, ScriptType.INLINE.getId(), talentId, "painless",
//                        Collections.singletonMap("extractText", extractText), "talent", adapter);
//            } catch (IOException | OutOfStringLengthException e) {
//                logger.warn("talent.reportAttachment error. table={},talentId={},url={},error={}", getListenerTable(), talentId, url, e.toString());
//            }
//        }
//    }
//}
