/*
 * Copyright 2017 viswadas leher .
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.dydabo.blackbox.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @author viswadas leher
 */
public class RedisConnectionManager {

    private static final Map<String, JedisPool> pools = new HashMap<>();

    private RedisConnectionManager() {
        //
    }

    public static Jedis getConnection(String hostName) {
        JedisPool jedisPool = pools.computeIfAbsent(hostName, n -> new JedisPool(new JedisPoolConfig(), n));
        return jedisPool.getResource();
    }
}
