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
package com.dydabo.blackbox.common;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Logger;

/**
 * Abstract Implementation of BlackBox
 *
 * @author viswadas leher
 */
public abstract class AbstractBlackBoxImpl<T extends BlackBoxable> implements BlackBox<T> {
    private final Logger logger = Logger.getLogger(getClass().getName());

    private final ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();

    public ForkJoinPool getForkJoinPool() {
        return forkJoinPool;
    }

    @Override
    public boolean delete(T row) throws BlackBoxException {
        logger.entering(getClass().getName(), "delete", row.toString());
        return delete(Collections.singletonList(row));
    }

    @Override
    public List<T> fetch(T row) throws BlackBoxException {
        logger.entering(getClass().getName(), "fetch", row.toString());
        return fetch(Collections.singletonList(row));
    }

    @Override
    public List<T> fetchByPartialKey(List<T> rows) throws BlackBoxException {
        logger.entering(getClass().getName(), "fetchByPartialKey", rows);
        return fetchByPartialKey(rows, -1);
    }

    @Override
    public List<T> fetchByPartialKey(T row) throws BlackBoxException {
        logger.entering(getClass().getName(), "fetchByPartialKey", row);
        return fetchByPartialKey(row, -1);
    }

    @Override
    public List<T> fetchByPartialKey(T row, long maxResults) throws BlackBoxException {
        logger.entering(getClass().getName(), "fetchByPartialKey", row + " :" + maxResults);
        return fetchByPartialKey(Collections.singletonList(row), -1);
    }

    @Override
    public boolean insert(T row) throws BlackBoxException {
        logger.entering(getClass().getName(), "insert", row);
        return insert(Collections.singletonList(row));
    }

    @Override
    public List<T> search(List<T> rows) throws BlackBoxException {
        logger.entering(getClass().getName(), "search", rows);
        return search(rows, -1);
    }

    @Override
    public List<T> search(T row) throws BlackBoxException {
        logger.entering(getClass().getName(), "search", row);
        return search(row, -1);
    }

    @Override
    public List<T> search(T row, long maxResults) throws BlackBoxException {
        logger.entering(getClass().getName(), "search", row + " :" + maxResults);
        return search(Collections.singletonList(row), maxResults);
    }

    @Override
    public List<T> search(T startRow, T endRow) throws BlackBoxException {
        logger.entering(getClass().getName(), "search", startRow + " :" + endRow);
        return search(startRow, endRow, -1);
    }

    @Override
    public boolean update(T newRow) throws BlackBoxException {
        logger.entering(getClass().getName(), "update", newRow);
        return update(Collections.singletonList(newRow));
    }
}
