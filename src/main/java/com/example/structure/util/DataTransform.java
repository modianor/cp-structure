package com.example.structure.util;

import com.alibaba.fastjson.JSONObject;
import com.cp.common.bean.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.io.ClassPathResource;
import sun.misc.BASE64Encoder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.cp.common.contant.Names.TOPIC_ORI_TEMPLATE;

public class DataTransform {
    public static ConsumerRecord<String, String> getRecordFromZipFile(String path) throws Exception {
        File file = new File(path);
        return getRecordFromZipFile(file);
    }

    public static ConsumerRecord<String, String> getRecordFromZipFile(File file) throws Exception {
        if (!file.exists()) {
            throw new Exception(String.format("文件路径：%s, 不存在!", file.getAbsolutePath()));
        }

        byte[] bytes = Files.readAllBytes(Paths.get(file.getAbsolutePath()));
        String data = new BASE64Encoder().encode(bytes);
        Event event = new Event().setTaskId("995031854")
                .setPolicyId("HENAN_ENV_PUNISHMENT")
                .setBisName("HENAN_JUNXIAN")
                .setData(data);

        return recordFromEvent(event);
    }

    public static ConsumerRecord<String, String> recordFromEvent(Event event) {
        String recordString = JSONObject.toJSONString(event);
        String topic = String.format(TOPIC_ORI_TEMPLATE, "HENAN_ENV_PUNISHMENT");
        String key = event.getTaskId();
        return new ConsumerRecord<String, String>(topic, 1, 1, key, recordString);
    }
}
