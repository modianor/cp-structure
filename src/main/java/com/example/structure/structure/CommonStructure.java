package com.example.structure.structure;

import cn.wanghaomiao.xpath.model.JXDocument;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cp.common.bean.Event;
import com.cp.common.util.ZipUtil;
import com.example.structure.bean.CommonConfig;
import com.example.structure.event.EventProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jsoup.nodes.Element;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.cp.common.contant.Names.TOPIC_BIS_TEMPLATE;
import static com.example.structure.util.DataTransform.getRecordFromZipFile;
import static com.example.structure.util.DataTransform.recordFromEvent;

@Slf4j
@Component
public class CommonStructure extends BasicStructure {
    @Autowired
    private EventProducer eventProducer;

    @Override
    public boolean verify(ConsumerRecord<String, String> record) {
        if (record == null || record.value() == null) {
            log.error("消息的内容为空!");
            return false;
        }
        return true;
    }

    @Override
    public Event tryTransform(ConsumerRecord<String, String> record) {
        if (!verify(record)) return null;

        Event event = JSONObject.parseObject(record.value().toString(), Event.class);
        if (event == null) {
            log.error("消息格式错误!");
            return null;
        }
        return event;
    }

    @Override
    public Event doOriStructure(ConsumerRecord<String, String> record) throws IOException {
        try {
            Event event = tryTransform(record);
            if (event == null) return null;

            String content = event.getData();
            String policyId = event.getPolicyId();
            Map<String, String> map = ZipUtil.unzip(content);
            JSONObject obj = JSONObject.parseObject(JSON.toJSONString(map));
            log.info("Kafka处理原始结构化Event:" + event.toString().substring(0, 1000));
            String bisTopic = String.format(TOPIC_BIS_TEMPLATE, policyId);
            event.setTopic(bisTopic);
            event.setData("");
            event.setObj(obj);
            return event;
        } catch (IOException | IllegalArgumentException e) {
            e.printStackTrace();
            return null;
        }
    }


    @Override
    public Event doBisStructure(ConsumerRecord<String, String> record) {
        try {
            Event event = tryTransform(record);
            if (event == null) return null;

            JSONObject obj = event.getObj();
            JSONObject collection = new JSONObject();
            String policyId = event.getPolicyId();
            String bisName = event.getBisName();
            String taskId = event.getTaskId();

            CommonConfig config = new CommonConfig(policyId, bisName);
            Map<String, JSONObject> nodes = config.getNodes();

            for (String patternName : nodes.keySet()) {
                // 遍历文件名称模式
                JSONObject rules = nodes.get(patternName);
                Map<String, Object> items = obj.getInnerMap();
                for (String fileName : items.keySet()) {
                    Pattern pattern = Pattern.compile(patternName);
                    Matcher matcher = pattern.matcher(fileName);
                    if (matcher.find()) {
                        String content = obj.getString(fileName);
                        for (String name : rules.keySet()) {
                            JSONObject rule = rules.getJSONObject(name);
                            try {
                                extract(content, collection, name, rule);
                            } catch (Exception e) {
                                log.error(String.format("Kafka业务结构化Event出错, 字段=%s, policyId=%s, bisName=%s, taskId=%s", name, policyId, bisName, taskId));
                            }
                        }
                    }
                    break;
                }
            }
            event.setObj(collection);
            log.info("Kafka处理业务结构化Event:" + event.toString());
            return event;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    @Override
    @KafkaListener(topics = "#{'${ori_common_topics}'.split(';')}")
    public void doOriStructure(ConsumerRecord<String, String> record, Acknowledgment ack) throws IOException {
        try {
            Event event = doOriStructure(record);
            eventProducer.fireEvent(event);
            ack.acknowledge();
        } catch (IOException | IllegalArgumentException e) {
            e.printStackTrace();
        }
    }

    @Override
    @KafkaListener(topics = "#{'${bis_common_topics}'.split(';')}")
    public void doBisStructure(ConsumerRecord<String, String> record, Acknowledgment ack) throws IOException {
        try {
            Event event = doBisStructure(record);
            ack.acknowledge();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void extract(String content, JSONObject collection, String fieldName, JSONObject rule) {
        String xpathRule = (String) rule.getOrDefault("xpath", "");
        if (xpathRule != null && !"".equals(xpathRule)) {
            // xpath解析
            JXDocument jxDocument = new JXDocument(content);
            Object rs = jxDocument.selOne(xpathRule);
            collection.put(fieldName, ((Element) rs).text());
            return;
        }

        String regexhRule = (String) rule.getOrDefault("regex", "");
        if (regexhRule != null && !"".equals(regexhRule)) {
            // regex解析
            return;
        }
    }

    public static void main(String[] args) throws Exception {
        String path = "/Users/wanghui/intellij_workspace/cp-structure/src/main/resources/OriData/HEIMAOTOUSU/HEIMAOTOUSU/995031854.zip";
        ConsumerRecord<String, String> record = getRecordFromZipFile(path);
        CommonStructure commonStructure = new CommonStructure();
        Event event = commonStructure.doOriStructure(record);
        record = recordFromEvent(event);
        event = commonStructure.doBisStructure(record);
        System.out.println(event);
    }
}
