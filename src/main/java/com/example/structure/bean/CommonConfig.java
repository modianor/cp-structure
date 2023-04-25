package com.example.structure.bean;

import com.alibaba.fastjson.JSONObject;
import org.springframework.util.ResourceUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class CommonConfig {
    private String config;
    private Map<String, JSONObject> nodes;

    public CommonConfig(String policyId, String bisName) throws IOException {
        this.config = getConfigString(policyId, bisName);
        parsePattern();
    }

    private String getConfigString(String policyId, String bisName) throws IOException {
        String path = "classpath:CommonConfig" + File.separator + policyId + File.separator + bisName + ".json";
        File file = ResourceUtils.getFile(path);
        InputStreamReader isr = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
        return readStringFromIO(isr);
    }

    private String readStringFromIO(InputStreamReader inputStream) throws IOException {
        char[] buffer = new char[1024];
        StringBuffer sb = new StringBuffer();
        int nums = 0;
        while ((nums = inputStream.read(buffer)) != -1) {
            sb.append(new String(buffer));
        }
        return sb.toString();
    }

    private void parseNodes() {
        nodes = new HashMap<>();
        JSONObject configObj = JSONObject.parseObject(this.config);
        JSONObject patternObj = (JSONObject) configObj.get("pattern");
        Map<String, Object> items = patternObj.getInnerMap();
        for (String key : items.keySet()) {
            JSONObject value = (JSONObject) items.get(key);
            nodes.put(key, value);
        }
    }

    private void parsePattern() {
        nodes = new HashMap<>();
        JSONObject configObj = JSONObject.parseObject(this.config);
        JSONObject patternObj = (JSONObject) configObj.get("pattern");
        Map<String, Object> items = patternObj.getInnerMap();
        for (String key : items.keySet()) {
            JSONObject value = (JSONObject) items.get(key);
            nodes.put(key, value);
        }
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    public Map<String, JSONObject> getNodes() {
        return this.nodes;
    }

    public void setNodes(Map<String, JSONObject> nodes) {
        this.nodes = nodes;
    }
}
