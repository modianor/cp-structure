package com.example.structure.config;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedList;
import java.util.List;

import static com.cp.common.contant.Names.TOPIC_BIS_TEMPLATE;
import static com.cp.common.contant.Names.TOPIC_ORI_TEMPLATE;

@Configuration
public class KafkaTopicConfig implements InitializingBean {

    @Value("${spider.framework.common_topics}")
    private String common_topics;

    @Override
    public void afterPropertiesSet() {
        //获取topic
        String[] topics = common_topics.split(";");
        List<String> oriTopics = new LinkedList<>();
        List<String> bisTopics = new LinkedList<>();

        for (String topic : topics) {
            String oriTopic = String.format(TOPIC_ORI_TEMPLATE, topic);
            String bisTopic = String.format(TOPIC_BIS_TEMPLATE, topic);
            oriTopics.add(oriTopic);
            bisTopics.add(bisTopic);
        }

        String oriTopicsStr = String.join(";", oriTopics);
        String bisTopicsStr = String.join(";", bisTopics);

        System.setProperty("ori_common_topics", oriTopicsStr);
        System.setProperty("bis_common_topics", bisTopicsStr);
    }


}
