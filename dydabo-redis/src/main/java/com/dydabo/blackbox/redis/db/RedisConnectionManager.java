/*
 *  Copyright 2020 viswadas leher <vleher@gmail.com>.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.dydabo.blackbox.redis.db;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author viswadas leher
 */
public class RedisConnectionManager {
    public static final int MAX_TOTAL = 100;
    private static final Logger logger = LogManager.getLogger();
    private static Map<String, RedisClient> redisClientPool = new ConcurrentHashMap<>();
    private final String hostName;
    private final Integer port;
    private final Integer database;

    public RedisConnectionManager(String hostName, Integer port, Integer database) {
        this.hostName = hostName;
        this.port = port;
        this.database = database;
    }

    private static StatefulRedisConnection<String, String> getConnection(String hostName, Integer port, Integer database) {
        RedisClient redisClient = redisClientPool.computeIfAbsent(hostName, name -> RedisClient.create("redis://" + hostName +
                ":" + port + "/" + database));
        return redisClient.connect();
    }

    public synchronized StatefulRedisConnection<String, String> getConnection() {
        return RedisConnectionManager.getConnection(hostName, port, database);
    }
}
