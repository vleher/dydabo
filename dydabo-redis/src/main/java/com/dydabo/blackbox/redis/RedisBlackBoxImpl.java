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

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.AbstractBlackBoxImpl;
import com.dydabo.blackbox.redis.db.RedisConnectionManager;
import com.dydabo.blackbox.redis.tasks.RedisDeleteTask;
import com.dydabo.blackbox.redis.tasks.RedisFetchTask;
import com.dydabo.blackbox.redis.tasks.RedisInsertTask;
import com.dydabo.blackbox.redis.tasks.RedisRangeSearchTask;
import com.dydabo.blackbox.redis.tasks.RedisSearchTask;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author viswadas leher
 */
public class RedisBlackBoxImpl<T extends BlackBoxable> extends AbstractBlackBoxImpl<T> implements BlackBox<T> {

    @Override
    public boolean delete(List<T> rows) throws BlackBoxException {
        RedisDeleteTask<T> delJob = new RedisDeleteTask<>(rows);
        return getForkJoinPool().invoke(delJob);
    }

    @Override
    public List<T> fetch(List<String> rowKeys, T bean) throws BlackBoxException {
        RedisFetchTask<T> fetchJob = new RedisFetchTask<>(rowKeys, bean, false, -1);
        return getForkJoinPool().invoke(fetchJob);
    }

    @Override
    public List<T> fetchByPartialKey(List<String> rowKeys, T bean, long maxResults) throws BlackBoxException {
        RedisFetchTask<T> fetchTask = new RedisFetchTask<>(rowKeys, bean, true, maxResults);
        return getForkJoinPool().invoke(fetchTask);
    }

    @Override
    public boolean insert(List<T> rows) throws BlackBoxException {
        RedisInsertTask<T> insertTask = new RedisInsertTask<>(rows, true);
        return getForkJoinPool().invoke(insertTask);
    }

    @Override
    public List<T> search(List<T> rows, long maxResults) throws BlackBoxException {
        RedisSearchTask<T> searchTask = new RedisSearchTask<>(rows, maxResults);
        return getForkJoinPool().invoke(searchTask);
    }

    @Override
    public List<T> search(T startRow, T endRow, long maxResults) throws BlackBoxException {
        RedisRangeSearchTask<T> rangeTask = new RedisRangeSearchTask<>(startRow, endRow, maxResults);
        return getForkJoinPool().invoke(rangeTask);
    }

    @Override
    public boolean update(List<T> newRows) throws BlackBoxException {
        RedisInsertTask<T> updateTask = new RedisInsertTask<>(newRows, false);
        return getForkJoinPool().invoke(updateTask);
    }

    protected Jedis getConnection() {
        return RedisConnectionManager.getConnection("localhost");
    }
}
