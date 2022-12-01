/*
 * Copyright 2022 Apollo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.ctrip.framework.apollo.enums;

/**
 * NacosConfigSourceType
 * @author lishuo
 * @date 2022/12/1 11:13
 **/
public enum NacosConfigSourceType {
  YAML("yaml","yaml"),
  YML("yml","yml"),
  XML("xml","xml"),
  PROPERTIES("properties","properties"),
  JSON("json","json");

  private String value;

  private String describe;

  NacosConfigSourceType(String value, String describe) {
    this.value = value;
    this.describe = describe;
  }

  public String getValue() {
    return value;
  }

  public String getDescribe() {
    return describe;
  }

  public static NacosConfigSourceType getEnumByMsg(String msg) {
    NacosConfigSourceType[] nacosConfigSourceTypes = values();
    for (NacosConfigSourceType nacosConfigSourceType : nacosConfigSourceTypes) {
      if(nacosConfigSourceType.getDescribe().equals(msg)){
        return nacosConfigSourceType;
      }
    }
    return null;
  }
}
