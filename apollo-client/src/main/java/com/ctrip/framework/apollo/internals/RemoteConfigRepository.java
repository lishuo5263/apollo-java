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
package com.ctrip.framework.apollo.internals;

import com.alibaba.fastjson.JSON;
import com.ctrip.framework.apollo.Apollo;
import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.dto.ApolloConfig;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;
import com.ctrip.framework.apollo.core.dto.ServiceDTO;
import com.ctrip.framework.apollo.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.framework.apollo.core.schedule.SchedulePolicy;
import com.ctrip.framework.apollo.core.signature.Signature;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.core.utils.DeferredLoggerFactory;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.ctrip.framework.apollo.enums.ConfigSourceType;
import com.ctrip.framework.apollo.enums.NacosConfigSourceType;
import com.ctrip.framework.apollo.exceptions.ApolloConfigException;
import com.ctrip.framework.apollo.exceptions.ApolloConfigStatusCodeException;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.ctrip.framework.apollo.util.http.*;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.escape.Escaper;
import com.google.common.io.CharStreams;
import com.google.common.net.UrlEscapers;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class RemoteConfigRepository extends AbstractConfigRepository {
    private static final Logger logger = DeferredLoggerFactory.getLogger(RemoteConfigRepository.class);
    private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
    private static final Joiner.MapJoiner MAP_JOINER = Joiner.on("&").withKeyValueSeparator("=");
    private static final Escaper pathEscaper = UrlEscapers.urlPathSegmentEscaper();
    private static final Escaper queryParamEscaper = UrlEscapers.urlFormParameterEscaper();

    private final ConfigServiceLocator m_serviceLocator;
    private final HttpClient m_httpClient;
    private final ConfigUtil m_configUtil;
    private final RemoteConfigLongPollService remoteConfigLongPollService;
    private volatile AtomicReference<ApolloConfig> m_configCache;
    private final String m_namespace;
    private final static ScheduledExecutorService m_executorService;
    private final AtomicReference<ServiceDTO> m_longPollServiceDto;
    private final AtomicReference<ApolloNotificationMessages> m_remoteMessages;
    private final RateLimiter m_loadConfigRateLimiter;
    private final AtomicBoolean m_configNeedForceRefresh;
    private final SchedulePolicy m_loadConfigFailSchedulePolicy;
    private static final Gson GSON = new Gson();

    static {
        m_executorService = Executors.newScheduledThreadPool(1,
                ApolloThreadFactory.create("RemoteConfigRepository", true));
    }

    /**
     * Constructor.
     *
     * @param namespace the namespace
     */
    public RemoteConfigRepository(String namespace) {
        m_namespace = namespace;
        m_configCache = new AtomicReference<>();
        m_configUtil = ApolloInjector.getInstance(ConfigUtil.class);
        m_httpClient = ApolloInjector.getInstance(HttpClient.class);
        m_serviceLocator = ApolloInjector.getInstance(ConfigServiceLocator.class);
        remoteConfigLongPollService = ApolloInjector.getInstance(RemoteConfigLongPollService.class);
        m_longPollServiceDto = new AtomicReference<>();
        m_remoteMessages = new AtomicReference<>();
        m_loadConfigRateLimiter = RateLimiter.create(m_configUtil.getLoadConfigQPS());
        m_configNeedForceRefresh = new AtomicBoolean(true);
        m_loadConfigFailSchedulePolicy = new ExponentialSchedulePolicy(m_configUtil.getOnErrorRetryInterval(),
                m_configUtil.getOnErrorRetryInterval() * 8);
        this.trySync();
        this.schedulePeriodicRefresh();
        this.scheduleLongPollingRefresh();
    }

    @Override
    public Properties getConfig() throws IOException {
        if (m_configCache.get() == null) {
            this.sync();
        }
        return transformApolloConfigToProperties(m_configCache.get());
    }

    @Override
    public void setUpstreamRepository(ConfigRepository upstreamConfigRepository) {
        //remote config doesn't need upstream
    }

    @Override
    public ConfigSourceType getSourceType() {
        return ConfigSourceType.REMOTE;
    }

    private void schedulePeriodicRefresh() {
        logger.debug("Schedule periodic refresh with interval: {} {}",
                m_configUtil.getRefreshInterval(), m_configUtil.getRefreshIntervalTimeUnit());
        m_executorService.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        Tracer.logEvent("Apollo.ConfigService", String.format("periodicRefresh: %s", m_namespace));
                        logger.debug("refresh config for namespace: {}", m_namespace);
                        trySync();
                        Tracer.logEvent("Apollo.Client.Version", Apollo.VERSION);
                    }
                }, m_configUtil.getRefreshInterval(), m_configUtil.getRefreshInterval(),
                m_configUtil.getRefreshIntervalTimeUnit());
    }

    @Override
    protected synchronized void sync() throws IOException {
        Transaction transaction = Tracer.newTransaction("Apollo.ConfigService", "syncRemoteConfig");

        try {
            ApolloConfig previous = m_configCache.get();
            ApolloConfig current = loadApolloConfig();

            //reference equals means HTTP 304
            if (previous != current) {
                logger.debug("Remote Config refreshed!");
                m_configCache.set(current);
                this.fireRepositoryChange(m_namespace, this.getConfig());
            }

            if (current != null) {
                Tracer.logEvent(String.format("Apollo.Client.Configs.%s", current.getNamespaceName()),
                        current.getReleaseKey());
            }

            transaction.setStatus(Transaction.SUCCESS);
        } catch (Throwable ex) {
            transaction.setStatus(ex);
            throw ex;
        } finally {
            transaction.complete();
        }
    }

    private Properties transformApolloConfigToProperties(ApolloConfig apolloConfig) {
        Properties result = propertiesFactory.getPropertiesInstance();
        result.putAll(apolloConfig.getConfigurations());
        return result;
    }

    private ApolloConfig loadApolloConfig() throws IOException {
        if (!m_loadConfigRateLimiter.tryAcquire(5, TimeUnit.SECONDS)) {
            //wait at most 5 seconds
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
            }
        }
        String appId = m_configUtil.getAppId();
        String cluster = m_configUtil.getCluster();
        String dataCenter = m_configUtil.getDataCenter();
        String secret = m_configUtil.getAccessKeySecret();
        Tracer.logEvent("Apollo.Client.ConfigMeta", STRING_JOINER.join(appId, cluster, m_namespace));
        int maxRetries = m_configNeedForceRefresh.get() ? 2 : 1;
        long onErrorSleepTime = 0; // 0 means no sleep
        Throwable exception = null;

        //apollo eureka instance
        List<ServiceDTO> configServices = getConfigServices();
        String url = null;
        retryLoopLabel:
        for (int i = 0; i < maxRetries; i++) {
            List<ServiceDTO> randomConfigServices = Lists.newLinkedList(configServices);
            Collections.shuffle(randomConfigServices);
            //Access the server which notifies the client first
            if (m_longPollServiceDto.get() != null) {
                randomConfigServices.add(0, m_longPollServiceDto.getAndSet(null));
            }

            for (ServiceDTO configService : randomConfigServices) {
                if (onErrorSleepTime > 0) {
                    logger.warn(
                            "Load config failed, will retry in {} {}. appId: {}, cluster: {}, namespaces: {}",
                            onErrorSleepTime, m_configUtil.getOnErrorRetryIntervalTimeUnit(), appId, cluster, m_namespace);

                    try {
                        m_configUtil.getOnErrorRetryIntervalTimeUnit().sleep(onErrorSleepTime);
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }

                url = assembleQueryConfigUrl(configService.getHomepageUrl(), appId, cluster, m_namespace,
                        dataCenter, m_remoteMessages.get(), m_configCache.get());

                logger.debug("Loading config from {}", url);
                Transaction transaction = Tracer.newTransaction("Apollo.ConfigService", "queryConfig");


                //exceptNamespace is not load nacos config
                if (Optional.ofNullable(System.getProperty("format")).isPresent() && Optional.ofNullable(System.getProperty("nacosLoadNamespace")).isPresent()) {
                    final String[] split = System.getProperty("nacosLoadNamespace").split(",");
                    for (String s : split) {
                        if (m_namespace.equalsIgnoreCase(s)) { // need load nacos
                            //nacos config fetch
                            //namespace -> dataId
                            //appId -> group
                            //multiple cluster -> appId + cluster
                            //http://127.0.0.1:8848/nacos/v1/cs/configs?dataId=broker-application.yml&group=base-common-default&tenant=hk-local-sync-pro
                            url = "http://127.0.0.1:8848/nacos/v1/cs/configs?dataId="
                                    + m_namespace + "&group=" + appId + "-" + cluster + "&tenant=" + System.getProperty("NacosNamespace");

                            HttpRequest request = new HttpRequest(url);
                            HttpNacosResponse response = doGetWithSerializeFunction(request);
                            String nacosResult = (String) response.getNacosConfigResult();
                            logger.info("Loaded nacos config for {}: {}", m_namespace, nacosResult);

                            if (split.length > 0) {
                                //todo ex: abcd ,  c d use nacos ,here need remove c & d
                                //System.setProperty("exceptNamespace",split);
                            }

                            NacosConfigSourceType nacosConfigSourceType = NacosConfigSourceType.getEnumByMsg(System.getProperty("format"));
                            Map<String, Object> configurations = new HashMap<>();
                            switch (Objects.requireNonNull(nacosConfigSourceType)) {
                                case YAML:
                                case YML:
                                    //configurations = YmlToProperties.ymlToProperties(nacosResult);
                                    configurations.put("content", nacosResult);
                                    break;
                                case PROPERTIES:
                                    Properties proper = new Properties();
                                    proper.load(new StringReader(nacosResult));
                                    Enumeration enum1 = proper.propertyNames();
                                    while (enum1.hasMoreElements()) {
                                        String strKey = (String) enum1.nextElement();
                                        String strValue = proper.getProperty(strKey);
                                        configurations.put(strKey, strValue);
                                    }
                                    break;
                            }

                            return ApolloConfig.builder()
                                    .namespaceName(m_namespace)
                                    .appId(appId)
                                    .cluster(cluster)
                                    .configurations(configurations)
                                    .build();
                        } else { //use load apollo config
                            //apollo fetch
                            return originalGetApolloConfig(appId, secret, url, transaction, exception, cluster, onErrorSleepTime);
                        }
                    }
                } else {
                    //this code use original load apollo config logic not use -D param
                    return originalGetApolloConfig(appId, secret, url, transaction, exception, cluster, onErrorSleepTime);
                }
            }
        }
        String message = String.format(
                "Load Apollo Config failed - appId: %s, cluster: %s, namespace: %s, url: %s",
                appId, cluster, m_namespace, url);
        throw new ApolloConfigException(message, exception);
    }

    private ApolloConfig originalGetApolloConfig(String appId, String secret, String url, Transaction transaction,Throwable exception,String cluster, long onErrorSleepTime) {
        try {
            HttpRequest request = new HttpRequest(url);
            if (!StringUtils.isBlank(secret)) {
                Map<String, String> headers = Signature.buildHttpHeaders(url, appId, secret);
                request.setHeaders(headers);
            }

            transaction.addData("Url", url);
            HttpResponse<ApolloConfig> response = m_httpClient.doGet(request, ApolloConfig.class);
            m_configNeedForceRefresh.set(false);
            m_loadConfigFailSchedulePolicy.success();

            transaction.addData("StatusCode", response.getStatusCode());
            transaction.setStatus(Transaction.SUCCESS);

            if (response.getStatusCode() == 304) {
                logger.debug("Config server responds with 304 HTTP status code.");
                return m_configCache.get();
            }

            ApolloConfig result = (ApolloConfig) response.getBody();

            logger.debug("Loaded config for {}: {}", m_namespace, result);

            return result;
        } catch (ApolloConfigStatusCodeException ex) {
            ApolloConfigStatusCodeException statusCodeException = ex;
            //config not found
            if (ex.getStatusCode() == 404) {
                String message = String.format(
                        "Could not find config for namespace - appId: %s, cluster: %s, namespace: %s, " +
                                "please check whether the configs are released in Apollo!",
                        appId, cluster, m_namespace);
                statusCodeException = new ApolloConfigStatusCodeException(ex.getStatusCode(),
                        message);
            }
            Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(statusCodeException));
            transaction.setStatus(statusCodeException);
            exception = statusCodeException;
        } catch (Throwable ex) {
            Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(ex));
            transaction.setStatus(ex);
            exception = ex;
        } finally {
            transaction.complete();
        }
        // if force refresh, do normal sleep, if normal config load, do exponential sleep
        onErrorSleepTime = m_configNeedForceRefresh.get() ? m_configUtil.getOnErrorRetryInterval() :
                m_loadConfigFailSchedulePolicy.fail();
        return null;
    }

    private static <T> HttpNacosResponse doGetWithSerializeFunction(HttpRequest httpRequest) {
        InputStreamReader isr = null;
        InputStreamReader esr = null;
        int statusCode;
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(httpRequest.getUrl()).openConnection();

            conn.setRequestMethod("GET");

            Map<String, String> headers = httpRequest.getHeaders();
            if (headers != null && headers.size() > 0) {
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    conn.setRequestProperty(entry.getKey(), entry.getValue());
                }
            }

//            int connectTimeout = httpRequest.getConnectTimeout();
//            if (connectTimeout < 0) {
//                connectTimeout = m_configUtil.getConnectTimeout();
//            }
//
//            int readTimeout = httpRequest.getReadTimeout();
//            if (readTimeout < 0) {
//                readTimeout = m_configUtil.getReadTimeout();
//            }

            conn.setConnectTimeout(0);
            conn.setReadTimeout(0);

            conn.connect();

            statusCode = conn.getResponseCode();
            String response;

            try {
                isr = new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8);
                response = CharStreams.toString(isr);
            } catch (IOException ex) {
                /**
                 * according to https://docs.oracle.com/javase/7/docs/technotes/guides/net/http-keepalive.html,
                 * we should clean up the connection by reading the response body so that the connection
                 * could be reused.
                 */
                InputStream errorStream = conn.getErrorStream();

                if (errorStream != null) {
                    esr = new InputStreamReader(errorStream, StandardCharsets.UTF_8);
                    try {
                        CharStreams.toString(esr);
                    } catch (IOException ioe) {
                        //ignore
                    }
                }

                // 200 and 304 should not trigger IOException, thus we must throw the original exception out
                if (statusCode == 200 || statusCode == 304) {
                    throw ex;
                }
                // for status codes like 404, IOException is expected when calling conn.getInputStream()
                throw new ApolloConfigStatusCodeException(statusCode, ex);
            }

            if (statusCode == 200) {
                return new HttpNacosResponse(statusCode, response);
            }

            if (statusCode == 304) {
                return new HttpNacosResponse(statusCode, null);
            }
        } catch (ApolloConfigStatusCodeException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new ApolloConfigException("Could not complete get operation", ex);
        } finally {
            if (isr != null) {
                try {
                    isr.close();
                } catch (IOException ex) {
                    // ignore
                }
            }

            if (esr != null) {
                try {
                    esr.close();
                } catch (IOException ex) {
                    // ignore
                }
            }
        }

        throw new ApolloConfigStatusCodeException(statusCode,
                String.format("Get operation failed for %s", httpRequest.getUrl()));
    }

    String assembleQueryConfigUrl(String uri, String appId, String cluster, String namespace,
                                  String dataCenter, ApolloNotificationMessages remoteMessages, ApolloConfig previousConfig) {

        String path = "configs/%s/%s/%s";
        List<String> pathParams =
                Lists.newArrayList(pathEscaper.escape(appId), pathEscaper.escape(cluster),
                        pathEscaper.escape(namespace));
        Map<String, String> queryParams = Maps.newHashMap();

        if (previousConfig != null) {
            queryParams.put("releaseKey", queryParamEscaper.escape(previousConfig.getReleaseKey()));
        }

        if (!Strings.isNullOrEmpty(dataCenter)) {
            queryParams.put("dataCenter", queryParamEscaper.escape(dataCenter));
        }

        String localIp = m_configUtil.getLocalIp();
        if (!Strings.isNullOrEmpty(localIp)) {
            queryParams.put("ip", queryParamEscaper.escape(localIp));
        }

        String label = m_configUtil.getApolloLabel();
        if (!Strings.isNullOrEmpty(label)) {
            queryParams.put("label", queryParamEscaper.escape(label));
        }

        if (remoteMessages != null) {
            queryParams.put("messages", queryParamEscaper.escape(GSON.toJson(remoteMessages)));
        }

        String pathExpanded = String.format(path, pathParams.toArray());

        if (!queryParams.isEmpty()) {
            pathExpanded += "?" + MAP_JOINER.join(queryParams);
        }
        if (!uri.endsWith("/")) {
            uri += "/";
        }
        return uri + pathExpanded;
    }

    private void scheduleLongPollingRefresh() {
        remoteConfigLongPollService.submit(m_namespace, this);
    }

    public void onLongPollNotified(ServiceDTO longPollNotifiedServiceDto, ApolloNotificationMessages remoteMessages) {
        m_longPollServiceDto.set(longPollNotifiedServiceDto);
        m_remoteMessages.set(remoteMessages);
        m_executorService.submit(new Runnable() {
            @Override
            public void run() {
                m_configNeedForceRefresh.set(true);
                trySync();
            }
        });
    }

    private List<ServiceDTO> getConfigServices() {
        List<ServiceDTO> services = m_serviceLocator.getConfigServices();
        if (services.size() == 0) {
            throw new ApolloConfigException("No available config service");
        }

        return services;
    }
}
