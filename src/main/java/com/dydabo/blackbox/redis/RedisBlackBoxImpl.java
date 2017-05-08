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
import com.dydabo.blackbox.db.RedisConnectionManager;
import com.dydabo.blackbox.redis.tasks.RedisDeleteTask;
import com.dydabo.blackbox.redis.tasks.RedisFetchTask;
import com.dydabo.blackbox.redis.tasks.RedisInsertTask;
import org.mortbay.util.SingletonList;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

/**
 * @author viswadas leher
 */
public class RedisBlackBoxImpl<T extends BlackBoxable> implements BlackBox<T> {
    @Override
    public boolean delete(List<T> rows) throws BlackBoxException {
        ForkJoinPool fjPool = ForkJoinPool.commonPool();

        RedisDeleteTask<T> delJob = new RedisDeleteTask<T>(rows);
        return fjPool.invoke(delJob);
    }

    @Override
    public boolean delete(T row) throws BlackBoxException {
        return delete(SingletonList.newSingletonList(row));
    }

    @Override
    public List<T> fetch(List<String> rowKeys, T bean) throws BlackBoxException {
        ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();

        RedisFetchTask<T> fetchJob = new RedisFetchTask<T>(rowKeys, bean, false, -1);
        return forkJoinPool.invoke(fetchJob);
    }

    @Override
    public List<T> fetch(String rowKey, T bean) throws BlackBoxException {
        return fetch(Collections.singletonList(rowKey), bean);
    }

    @Override
    public List<T> fetchByPartialKey(List<String> rowKeys, T bean) throws BlackBoxException {
        return null;
    }

    @Override
    public List<T> fetchByPartialKey(List<String> rowKeys, T bean, long maxResults) throws BlackBoxException {
        return null;
    }

    @Override
    public List<T> fetchByPartialKey(String rowKey, T bean) throws BlackBoxException {
        return null;
    }

    @Override
    public List<T> fetchByPartialKey(String rowKey, T bean, long maxResults) throws BlackBoxException {
        return null;
    }

    @Override
    public boolean insert(List<T> rows) throws BlackBoxException {
        return false;
    }

    @Override
    public boolean insert(T row) throws BlackBoxException {
        return false;
    }

    @Override
    public List<T> search(List<T> rows) throws BlackBoxException {
        return null;
    }

    @Override
    public List<T> search(List<T> rows, long maxResults) throws BlackBoxException {
        return null;
    }

    @Override
    public List<T> search(T row) throws BlackBoxException {
        return null;
    }

    @Override
    public List<T> search(T row, long maxResults) throws BlackBoxException {
        return null;
    }

    @Override
    public List<T> search(T startRow, T endRow) throws BlackBoxException {
        return null;
    }

    @Override
    public List<T> search(T startRow, T endRow, long maxResults) throws BlackBoxException {
        return null;
    }

    @Override
    public boolean update(List<T> newRows) throws BlackBoxException {
        ForkJoinPool fjPool = ForkJoinPool.commonPool();
        RedisInsertTask<T> updateTask = new RedisInsertTask<T>(newRows, true);
        return fjPool.invoke(updateTask);
    }

    @Override
    public boolean update(T newRow) throws BlackBoxException {
        return update(SingletonList.newSingletonList(newRow));
    }

    protected Jedis getConnection() {
        return RedisConnectionManager.getConnection("localhost");
    }
}
