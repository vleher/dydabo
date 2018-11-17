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
package com.dydabo.blackbox.mongodb;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.AbstractBlackBoxImpl;
import com.dydabo.blackbox.mongodb.db.MongoDBConnectionManager;
import com.dydabo.blackbox.mongodb.tasks.*;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.List;

/**
 * @param <T>
 * @author viswadas leher
 */
public class MongoBlackBoxImpl<T extends BlackBoxable> extends AbstractBlackBoxImpl<T> implements BlackBox<T> {

    @Override
    public boolean delete(List<T> rows) throws BlackBoxException {
        MongoDeleteTask<T> task = new MongoDeleteTask<>(getCollection(), rows);
        return task.invoke();
    }

    @Override
    public List<T> fetch(List<T> rows) throws BlackBoxException {
        MongoFetchTask<T> task = new MongoFetchTask<>(getCollection(), rows, false, -1);
        return task.invoke();
    }

    @Override
    public List<T> fetchByPartialKey(List<T> rows,  long maxResults) throws BlackBoxException {
        MongoFetchTask<T> task = new MongoFetchTask<>(getCollection(), rows,  true, maxResults);
        return task.invoke();
    }

    @Override
    public boolean insert(List<T> rows) throws BlackBoxException {
        MongoInsertTask<T> task = new MongoInsertTask<>(getCollection(), rows, true);
        return task.invoke();
    }


    @Override
    public List<T> search(List<T> rows, long maxResults) throws BlackBoxException {
        MongoSearchTask<T> task = new MongoSearchTask<>(getCollection(), rows, maxResults);
        return task.invoke();
    }


    @Override
    public List<T> search(T startRow, T endRow, long maxResults) throws BlackBoxException {
        MongoRangeSearchTask<T> task = new MongoRangeSearchTask<>(getCollection(), startRow, endRow, maxResults);
        return task.invoke();

    }

    @Override
    public boolean update(List<T> newRows) throws BlackBoxException {
        MongoInsertTask<T> task = new MongoInsertTask<>(getCollection(), newRows, false);
        return task.invoke();
    }

    /**
     * @return
     */
    private MongoCollection<Document> getCollection() {
        // TODO: make this configurable
        return MongoDBConnectionManager.getMongoDBCollection(null, "dydabo", "dydabo");
    }
}
