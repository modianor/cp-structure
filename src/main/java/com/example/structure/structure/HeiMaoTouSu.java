package com.example.structure.structure;

import cn.wanghaomiao.xpath.model.JXDocument;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cp.common.bean.Event;
import com.cp.common.util.ZipUtil;
import com.example.structure.event.EventProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jsoup.nodes.Element;
import org.seimicrawler.xpath.exception.XpathSyntaxErrorException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class HeiMaoTouSu extends BasicStructure {
    @Autowired
    private EventProducer eventProducer;

    public HeiMaoTouSu() {
        String policyId = "HEIMAOTOUSU";
    }

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
    @KafkaListener(topics = {"TP_BDG_AD_HEIMAOTOUSU_ORISTRUCT"})
    public Event doOriStructure(ConsumerRecord<String, String> record) throws IOException {
        if (!verify(record)) {
            return null;
        }

        Event event = JSONObject.parseObject(record.value().toString(), Event.class);
        if (event == null) {
            log.error("消息格式错误!");
            return null;
        }
        String content = event.getData();
        Map<String, String> map = ZipUtil.unzip(content);
        JSONObject obj = JSONObject.parseObject(JSON.toJSONString(map));
        log.info("Kafka处理原始结构化Event:" + event.toString().substring(0, 1000));

        event.setTopic("TP_BDG_AD_HEIMAOTOUSU_BISSTRUCT");
        event.setData("");
        event.setObj(obj);
        return event;
    }

    @Override
    @KafkaListener(topics = {"TP_BDG_AD_HEIMAOTOUSU_BISSTRUCT"})
    public Event doBisStructure(ConsumerRecord<String, String> record) throws IOException {
        if (!verify(record)) {
            return null;
        }

        Event event = JSONObject.parseObject(record.value().toString(), Event.class);
        if (event == null) {
            log.error("消息格式错误!");
            return null;
        }
        JSONObject obj = event.getObj();
        JSONObject data = this.parse(obj);
        event.setObj(data);
        log.info("Kafka处理业务结构化Event:" + data);
        return event;
    }

    public JSONObject parse(JSONObject obj) {
        JSONObject data = new JSONObject();
        try {
            String content = obj.getString("index.html");
            String titleXpath = "//h1[@class=\"article\"]/text()";
            String nameXpath = "//div[@class='ts-q-user clearfix']/span[@class='u-name']/text()";
            String idXpath = "//li/label[contains(text(),\"投诉编号\")]/parent::li/allText()";
            String targetXpath = "//li/label[contains(text(),\"投诉对象\")]/parent::li/a/allText()";
            String problemXpath = "//li/label[contains(text(),\"投诉问题\")]/parent::li/allText()";
            String appealXpath = "//li/label[contains(text(),\"投诉要求\")]/parent::li/allText()";
            String amountXpath = "//li/label[contains(text(),\"涉诉金额\")]/parent::li/allText()";
            String statusXpath = "//li/label[contains(text(),\"投诉进度\")]/parent::li/b/allText()";
            JXDocument jxDocument = new JXDocument(content);

            Object rs = jxDocument.selOne(titleXpath);
            data.put("title", ((Element) rs).text());

            rs = jxDocument.selOne(nameXpath);
            data.put("username", ((Element) rs).text());

            rs = jxDocument.selOne(idXpath);
            if (rs != null)
                data.put("complaint_id", rs);

            rs = jxDocument.selOne(targetXpath);
            if (rs != null)
                data.put("complaint_target", rs);

            rs = jxDocument.selOne(problemXpath);
            if (rs != null)
                data.put("complaint_problem", rs);

            rs = jxDocument.selOne(appealXpath);
            if (rs != null)
                data.put("complaint_appeal", rs);

            rs = jxDocument.selOne(amountXpath);
            if (rs != null)
                data.put("amount_involved", rs);

            rs = jxDocument.selOne(statusXpath);
            if (rs != null)
                data.put("status", rs);
        } catch (XpathSyntaxErrorException e) {
            e.printStackTrace();
        }

        return data;
    }
}
