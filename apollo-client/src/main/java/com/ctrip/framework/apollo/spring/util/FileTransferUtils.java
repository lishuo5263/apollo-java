package com.ctrip.framework.apollo.spring.util;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;

import java.util.HashMap;
import java.util.Map;

/**
 * @Title:
 * @Author lishuo
 * @Date 2022-12-01 15:13
 **/
public class FileTransferUtils {

    public static void main(String[] args) {
        Map<String, String> configurations =new HashMap<>();
        yml2Properties("james:\n" +
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
                "applicationName: applicationNamesdf1312",configurations);
        System.out.println(configurations);
    }

    public static void yml2Properties(String s, Map<String, String> configurations) {
        final String DOT = ".";
        try {
            YAMLFactory yamlFactory = new YAMLFactory();
            YAMLParser parser = yamlFactory.createParser(s);
            String key = "";
            String value ;
            JsonToken token = parser.nextToken();
            while (token != null) {
                if (JsonToken.START_OBJECT.equals(token)) {
                    // "{" 表示字符串开头，不做解析
                } else if (JsonToken.FIELD_NAME.equals(token)) {
                    if (key.length() > 0) {
                        key = key + DOT;
                    }
                    key = key + parser.getCurrentName();
                    token = parser.nextToken();
                    if (JsonToken.START_OBJECT.equals(token)) {
                        continue;
                    }
                    value = parser.getText();
                    //lines.add(key + "=" + value);
                    configurations.put(key,value);
                    int dotOffset = key.lastIndexOf(DOT);
                    if (dotOffset > 0) {
                        key = key.substring(0, dotOffset);
                    }
                } else if (JsonToken.END_OBJECT.equals(token)) {
                    int dotOffset = key.lastIndexOf(DOT);
                    if (dotOffset > 0) {
                        key = key.substring(0, dotOffset);
                    }
                }
                token = parser.nextToken();
            }
            parser.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
