package com.example.structure.structure;

import com.cp.common.bean.Event;
import com.example.structure.event.EventProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.Acknowledgment;

import java.io.IOException;

import static com.cp.common.contant.Names.TOPIC_BIS_TEMPLATE;
import static com.cp.common.contant.Names.TOPIC_ORI_TEMPLATE;


public abstract class BasicStructure {
    public String policyId = "";
    @Autowired
    private EventProducer eventProducer;

    public BasicStructure() {
    }

    public boolean verify(ConsumerRecord<String, String> record) {
        return true;
    }

    public Event tryTransform(ConsumerRecord<String, String> record) {
        return null;
    }

    public Event doOriStructure(ConsumerRecord<String, String> record) throws IOException {
        return null;
    }

    public void doOriStructure(ConsumerRecord<String, String> record, Acknowledgment ack) throws IOException {
    }


    public Event doBisStructure(ConsumerRecord<String, String> record) throws IOException {
        return null;
    }

    public void doBisStructure(ConsumerRecord<String, String> record, Acknowledgment ack) throws IOException {

    }

    public String getOriginStructureTopic() {
        return String.format(TOPIC_ORI_TEMPLATE, this.policyId);
    }

    public String getBusinessStructureTopic() {
        return String.format(TOPIC_BIS_TEMPLATE, this.policyId);
    }

}
