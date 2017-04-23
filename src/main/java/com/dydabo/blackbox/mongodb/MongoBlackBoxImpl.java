/*
 * Copyright 2017 viswadas leher <vleher@gmail.com>. Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License.
 */
package com.dydabo.blackbox.mongodb;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.db.MongoDBConnectionManager;
import com.dydabo.blackbox.mongodb.tasks.*;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.Collections;
import java.util.List;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 * @param <T>
 */
public class MongoBlackBoxImpl<T extends BlackBoxable> implements BlackBox<T> {

    @Override
    public boolean delete(List<T> rows) throws BlackBoxException {
        MongoDeleteTask<T> task = new MongoDeleteTask<>(getCollection(), rows);
        return task.invoke();
    }

    @Override
    public boolean delete(T row) throws BlackBoxException {
        return delete(Collections.singletonList(row));
    }

    @Override
    public List<T> fetch(List<String> rowKeys, T bean) throws BlackBoxException {
        MongoFetchTask<T> task = new MongoFetchTask<>(getCollection(), rowKeys, bean, false, -1);
        return task.invoke();
    }

    @Override
    public List<T> fetch(String rowKey, T bean) throws BlackBoxException {
        return fetch(Collections.singletonList(rowKey), bean);
    }

    @Override
    public List<T> fetchByPartialKey(List<String> rowKeys, T bean) throws BlackBoxException {
        return fetchByPartialKey(rowKeys, bean, -1);
    }

    @Override
    public List<T> fetchByPartialKey(List<String> rowKeys, T bean, long maxResults) throws BlackBoxException {
        MongoFetchTask<T> task = new MongoFetchTask<>(getCollection(), rowKeys, bean, true, maxResults);
        return task.invoke();
    }

    @Override
    public List<T> fetchByPartialKey(String rowKey, T bean) throws BlackBoxException {
        return fetchByPartialKey(Collections.singletonList(rowKey), bean, -1);
    }

    @Override
    public List<T> fetchByPartialKey(String rowKey, T bean, long maxResults) throws BlackBoxException {
        return fetchByPartialKey(Collections.singletonList(rowKey), bean, maxResults);
    }

    @Override
    public boolean insert(List<T> rows) throws BlackBoxException {
        MongoInsertTask<T> task = new MongoInsertTask<>(getCollection(), rows, true);
        return task.invoke();
    }

    @Override
    public boolean insert(T row) throws BlackBoxException {
        return insert(Collections.singletonList(row));
    }

    @Override
    public List<T> search(List<T> rows) throws BlackBoxException {
        return search(rows, -1);
    }

    @Override
    public List<T> search(List<T> rows, long maxResults) throws BlackBoxException {
        MongoSearchTask<T> task = new MongoSearchTask<T>(getCollection(), rows, maxResults);
        return task.invoke();
    }

    @Override
    public List<T> search(T row) throws BlackBoxException {
        return search(Collections.singletonList(row), -1);
    }

    @Override
    public List<T> search(T row, long maxResults) throws BlackBoxException {
        return search(Collections.singletonList(row), maxResults);
    }

    @Override
    public List<T> search(T startRow, T endRow) throws BlackBoxException {
        return search(startRow, endRow, -1);
    }

    @Override
    public List<T> search(T startRow, T endRow, long maxResults) throws BlackBoxException {
        MongoRangeSearchTask<T> task = new MongoRangeSearchTask<T>(getCollection(), startRow, endRow, maxResults);
        return task.invoke();

    }

    @Override
    public boolean update(List<T> newRows) throws BlackBoxException {
        MongoInsertTask<T> task = new MongoInsertTask<>(getCollection(), newRows, false);
        return task.invoke();
    }

    @Override
    public boolean update(T newRow) throws BlackBoxException {
        return update(Collections.singletonList(newRow));
    }

    /**
     *
     * @return
     */
    public MongoCollection<Document> getCollection() {
        // TODO: make this configurable
        return MongoDBConnectionManager.getMongoDBCollection(null, "dydabo", "dydabo");
    }
}
