/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.servicecomb.loadbalance;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.servicecomb.foundation.common.utils.SPIServiceUtils;
import org.apache.servicecomb.serviceregistry.api.registry.MicroserviceInstance;
import org.apache.servicecomb.serviceregistry.consumer.MicroserviceInstancePing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.netflix.config.DynamicPropertyFactory;

/**
 *  Add special stats that com.netflix.loadbalancer.LoadBalancerStats not provided
 */
public class ServiceCombLoadBalancerStats {
  private final static Logger LOGGER = LoggerFactory.getLogger(ServiceCombLoadBalancerStats.class);

  private final Map<String, ServiceCombServerStats> pingView = new ConcurrentHashMap<>();

  private int serverExpireInSeconds = DynamicPropertyFactory.getInstance()
      .getIntProperty(Configuration.RPOP_SERVER_EXPIRED_IN_SECONDS, 300).get();

  private long timerIntervalInMillis = DynamicPropertyFactory.getInstance()
      .getLongProperty(Configuration.RPOP_TIMER_INTERVAL_IN_MINIS, 10000).get();

  private LoadingCache<String, ServiceCombServerStats> serverStatsCache;

  public static final ServiceCombLoadBalancerStats INSTANCE;

  private Timer timer;

  static {
    INSTANCE = new ServiceCombLoadBalancerStats();
    INSTANCE.init();
  }

  /**
   * Should be singleton, use it only for testing
   */
  ServiceCombLoadBalancerStats() {
  }

  public void markIsolated(ServiceCombServer server, boolean isolated) {
    try {
      getServerStatusFromCache(server).markIsolated(isolated);
    } catch (ExecutionException e) {
      LOGGER.error("Not expected to happen, maybe a bug.", e);
    }
  }

  public void markSuccess(ServiceCombServer server) {
    try {
      getServerStatusFromCache(server).markSuccess();
    } catch (ExecutionException e) {
      LOGGER.error("Not expected to happen, maybe a bug.", e);
    }
  }

  public void markFailure(ServiceCombServer server) {
    try {
      getServerStatusFromCache(server).markFailure();
    } catch (ExecutionException e) {
      LOGGER.error("Not expected to happen, maybe a bug.", e);
    }
  }

  public ServiceCombServerStats getServiceCombServerStats(String instanceId) {
    try {
      return serverStatsCache.get(instanceId);
    } catch (ExecutionException e) {
      LOGGER.error("Not expected to happen, maybe a bug.", e);
      return null;
    }
  }

  public ServiceCombServer getServiceCombServer(MicroserviceInstance instance) {
    return getServiceCombServer(instance.getInstanceId());
  }

  public ServiceCombServer getServiceCombServer(String instanceId) {
    try {
      ServiceCombServerStats serviceCombServerStats = serverStatsCache.get(instanceId);
      if (serviceCombServerStats == null) {
        return null;
      }
      return serviceCombServerStats.getServiceCombServer();
    } catch (ExecutionException e) {
      LOGGER.error("Not expected to happen, maybe a bug.", e);
      return null;
    }
  }

  @VisibleForTesting
  void setServerExpireInSeconds(int sec) {
    this.serverExpireInSeconds = sec;
  }

  @VisibleForTesting
  void setTimerIntervalInMillis(int millis) {
    this.timerIntervalInMillis = millis;
  }

  private ServiceCombServerStats getServerStatusFromCache(ServiceCombServer server) throws ExecutionException {
    ServiceCombServerStats stats = serverStatsCache.get(server.getInstance().getInstanceId());
    if (stats.getServiceCombServer() == null) {
      stats.setServiceCombServer(server);
    }
    return stats;
  }

  @VisibleForTesting
  Map<String, ServiceCombServerStats> getPingView() {
    return this.pingView;
  }

  void init() {
    // for testing
    if (timer != null) {
      timer.cancel();
    }
    if (serverStatsCache != null) {
      serverStatsCache.cleanUp();
    }

    serverStatsCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(serverExpireInSeconds, TimeUnit.SECONDS)
            .removalListener(new RemovalListener<String, ServiceCombServerStats>() {
              @Override
              public void onRemoval(RemovalNotification<String, ServiceCombServerStats> notification) {
                LOGGER.info("stats of instance {} removed.", notification.getKey());
                pingView.remove(notification.getKey());
              }
            })
            .build(
                new CacheLoader<String, ServiceCombServerStats>() {
                  public ServiceCombServerStats load(String instanceId) {
                    ServiceCombServerStats stats = new ServiceCombServerStats();
                    pingView.put(instanceId, stats);
                    return stats;
                  }
                });

    timer = new Timer("LoadBalancerStatsTimer", true);
    timer.schedule(new TimerTask() {
      private MicroserviceInstancePing ping = SPIServiceUtils.getPriorityHighestService(MicroserviceInstancePing.class);

      @Override
      public void run() {
        try {
          Map<String, ServiceCombServerStats> allServers = pingView;
          allServers.entrySet().forEach(serviceCombServerServiceCombServerStatsEntry -> {
            String instanceId = serviceCombServerServiceCombServerStatsEntry.getKey();
            ServiceCombServer server = getServiceCombServer(instanceId);
            ServiceCombServerStats stats = serviceCombServerServiceCombServerStatsEntry.getValue();
            if ((System.currentTimeMillis() - stats.getLastVisitTime() > timerIntervalInMillis) && !ping
                .ping(server.getInstance())) {
              LOGGER.info("ping mark server {} failure.", server.getInstance().getInstanceId());
              stats.markFailure();
            }
          });
          serverStatsCache.cleanUp();
        } catch (Throwable e) {
          LOGGER.warn("LoadBalancerStatsTimer error.", e);
        }
      }
    }, timerIntervalInMillis, timerIntervalInMillis);
  }
}

