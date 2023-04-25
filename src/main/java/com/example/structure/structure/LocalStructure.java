package com.example.structure.structure;

import com.cp.common.bean.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import java.io.File;

import static com.example.structure.util.DataTransform.getRecordFromZipFile;
import static com.example.structure.util.DataTransform.recordFromEvent;

@Component
@Order(value = 1)
public class LocalStructure implements ApplicationRunner {

    @Value("${spider.framework.local_test_policyId}")
    private String policyId;
    @Value("${spider.framework.local_test_bisName}")
    private String bisName;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if ((policyId == null || bisName == null) || ("".equals(policyId) && "".equals(bisName))) return;

        String path = "classpath:OriData" + File.separator + policyId + File.separator + bisName;
        File dataDir = ResourceUtils.getFile(path);
        File[] files = dataDir.listFiles();
        for (File file : files) {
            ConsumerRecord<String, String> record = getRecordFromZipFile(file);
            CommonStructure commonStructure = new CommonStructure();
            Event event = commonStructure.doOriStructure(record);
            record = recordFromEvent(event);
            commonStructure.doBisStructure(record);
        }
        System.exit(0);
    }
}
