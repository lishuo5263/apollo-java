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
package com.ctrip.framework.apollo.mockserver;

import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.ApolloClientSystemConsts;
import com.ctrip.framework.apollo.core.dto.ApolloConfig;
import com.ctrip.framework.apollo.core.dto.ApolloConfigNotification;
import com.ctrip.framework.apollo.core.utils.ResourceUtils;
import com.ctrip.framework.apollo.internals.ConfigServiceLocator;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create by zhangzheng on 8/22/18 Email:zhangzheng@youzan.com
 */
public class EmbeddedApollo extends ExternalResource {

  private static final Logger logger = LoggerFactory.getLogger(EmbeddedApollo.class);
  private static final Type notificationType = new TypeToken<List<ApolloConfigNotification>>() {
  }.getType();

  private static Method CONFIG_SERVICE_LOCATOR_CLEAR;
  private static ConfigServiceLocator CONFIG_SERVICE_LOCATOR;

  private static final Gson GSON = new Gson();
  private final Map<String, Map<String, String>> addedOrModifiedPropertiesOfNamespace = Maps.newConcurrentMap();
  private final Map<String, Set<String>> deletedKeysOfNamespace = Maps.newConcurrentMap();

  private MockWebServer server;

  static {
    try {
      System.setProperty("apollo.longPollingInitialDelayInMills", "0");
      CONFIG_SERVICE_LOCATOR = ApolloInjector.getInstance(ConfigServiceLocator.class);
      CONFIG_SERVICE_LOCATOR_CLEAR = ConfigServiceLocator.class.getDeclaredMethod("initConfigServices");
      CONFIG_SERVICE_LOCATOR_CLEAR.setAccessible(true);
    } catch (NoSuchMethodException e) {
      logger.error(e.getMessage(), e);
    }
  }

  @Override
  protected void before() throws Throwable {
    clear();
    server = new MockWebServer();
    final Dispatcher dispatcher = new Dispatcher() {
      @Override
      public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
        if (request.getPath().startsWith("/notifications/v2")) {
          String notifications = request.getRequestUrl().queryParameter("notifications");
          return new MockResponse().setResponseCode(200).setBody(mockLongPollBody(notifications));
        }
        if (request.getPath().startsWith("/configs")) {
          List<String> pathSegments = request.getRequestUrl().pathSegments();
          // appId and cluster might be used in the future
          String appId = pathSegments.get(1);
          String cluster = pathSegments.get(2);
          String namespace = pathSegments.get(3);
          return new MockResponse().setResponseCode(200).setBody(loadConfigFor(namespace));
        }
        return new MockResponse().setResponseCode(404);
      }
    };

    server.setDispatcher(dispatcher);
    server.start();

    mockConfigServiceUrl("http://localhost:" + server.getPort());

    super.before();
  }

  @Override
  protected void after() {
    try {
      clear();
      server.close();
    } catch (Exception e) {
      logger.error("stop apollo server error", e);
    }
  }

  private void clear() throws Exception {
    resetOverriddenProperties();
  }

  private void mockConfigServiceUrl(String url) throws Exception {
    System.setProperty(ApolloClientSystemConsts.APOLLO_CONFIG_SERVICE, url);

    CONFIG_SERVICE_LOCATOR_CLEAR.invoke(CONFIG_SERVICE_LOCATOR);
  }

  private String loadConfigFor(String namespace) {
    String filename = String.format("mockdata-%s.properties", namespace);
    final Properties prop = ResourceUtils.readConfigFile(filename, new Properties());
    Map<String, String> configurations = Maps.newHashMap();
    for (String propertyName : prop.stringPropertyNames()) {
      configurations.put(propertyName, prop.getProperty(propertyName));
    }
    ApolloConfig apolloConfig = new ApolloConfig("someAppId", "someCluster", namespace, "someReleaseKey");

    Map<String, String> mergedConfigurations = mergeOverriddenProperties(namespace, configurations);
    //apolloConfig.setConfigurations(mergedConfigurations);
    return GSON.toJson(apolloConfig);
  }

  private String mockLongPollBody(String notificationsStr) {
    List<ApolloConfigNotification> oldNotifications = GSON.fromJson(notificationsStr, notificationType);
    List<ApolloConfigNotification> newNotifications = new ArrayList<>();
    for (ApolloConfigNotification notification : oldNotifications) {
      newNotifications
          .add(new ApolloConfigNotification(notification.getNamespaceName(), notification.getNotificationId() + 1));
    }
    return GSON.toJson(newNotifications);
  }

  /**
   * 合并用户对namespace的修改
   */
  private Map<String, String> mergeOverriddenProperties(String namespace, Map<String, String> configurations) {
    if (addedOrModifiedPropertiesOfNamespace.containsKey(namespace)) {
      configurations.putAll(addedOrModifiedPropertiesOfNamespace.get(namespace));
    }
    if (deletedKeysOfNamespace.containsKey(namespace)) {
      for (String k : deletedKeysOfNamespace.get(namespace)) {
        configurations.remove(k);
      }
    }
    return configurations;
  }

  /**
   * Add new property or update existed property
   */
  public void addOrModifyProperty(String namespace, String someKey, String someValue) {
    if (addedOrModifiedPropertiesOfNamespace.containsKey(namespace)) {
      addedOrModifiedPropertiesOfNamespace.get(namespace).put(someKey, someValue);
    } else {
      Map<String, String> m = Maps.newConcurrentMap();
      m.put(someKey, someValue);
      addedOrModifiedPropertiesOfNamespace.put(namespace, m);
    }
  }

  /**
   * Delete existed property
   */
  public void deleteProperty(String namespace, String someKey) {
    if (deletedKeysOfNamespace.containsKey(namespace)) {
      deletedKeysOfNamespace.get(namespace).add(someKey);
    } else {
      Set<String> m = Sets.newConcurrentHashSet();
      m.add(someKey);
      deletedKeysOfNamespace.put(namespace, m);
    }
  }

  /**
   * reset overridden properties
   */
  public void resetOverriddenProperties() {
    addedOrModifiedPropertiesOfNamespace.clear();
    deletedKeysOfNamespace.clear();
  }
}
