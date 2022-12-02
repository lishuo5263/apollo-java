package com.ctrip.framework.apollo.spring.util;


import com.alibaba.fastjson.JSONObject;
import org.yaml.snakeyaml.Yaml;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map;

/**
 * @Title:
 * @Description:
 * @Author: xbl
 * @CreateDate: 2021/10/27 19:08
 */
public class YmlToProperties {

    public static void main(String[] args) {
        Map<String, Object> resultMap = null;

        Map<String, Object> result = ymlToProperties("james:\n" +
                "  name: devops.lishuo-yaml.james.name-nacos\n" +
                "  age: '111'\n" +
                "environment: FAT\n" +
                "sample:\n" +
                "  timeout: '1000'\n" +
                "  size: '890234723'\n" +
                "test:\n" +
                "  input: ENC(Ore69lUopDHL5R8Bw/G3bQ==)\n" +
                "  input1: ckl\n" +
                "server:\n" +
                "  port: '9099'\n" +
                "  delay: '324234'\n" +
                "jasypt:\n" +
                "  encryptor:\n" +
                "    password: klklklklklklklkl\n" +
                "name2: devops.lishuo-yaml.name2-nacos\n" +
                "applicationName: applicationNamesdf1312");

        System.out.println(new JSONObject(result));
        System.out.println(result.get("james.name"));
    }

    public static Map<String, Object> ymlToProperties(String path) {
        Yaml yaml = new Yaml();
            Map<String, Object> m = yaml.load(path);
        return mapToProperties(m);
    }

    public static Map<String, Object> mapToProperties(Map<String, Object> m) {
        final Map<String, Object> resultMap = new LinkedHashMap<>();
        mapToProperties(null, resultMap, m);
        return resultMap;
    }

    private static void mapToProperties(String key, final Map<String, Object> toMap, Map<String, Object> fromMap) {
        for (Map.Entry<String, Object> entry : fromMap.entrySet()) {
            Object value = entry.getValue();
            String relKey = entry.getKey();
            if (key != null) {
                relKey = key + "." + entry.getKey();
            }
            if (value instanceof Map) {
                mapToProperties(relKey, toMap, (Map<String, Object>) value);
            } else {
                toMap.put(relKey, entry.getValue());
            }
        }
    }

}
