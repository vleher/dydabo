/*
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dydabo.blackbox.mongodb;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.db.MongoDBConnectionManager;
import com.dydabo.blackbox.mongodb.tasks.MongoDeleteTask;
import com.dydabo.blackbox.mongodb.tasks.MongoInsertTask;
import com.mongodb.async.client.MongoCollection;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class MongoBlackBoxImpl<T extends BlackBoxable> implements BlackBox<T> {

    @Override
    public boolean delete(List<T> rows) throws BlackBoxException {
        MongoDeleteTask<T> task = new MongoDeleteTask(getCollection(), rows);
        return task.invoke();
    }

    @Override
    public boolean delete(T row) throws BlackBoxException {
        return delete(Arrays.asList(row));
    }

    @Override
    public List<T> fetch(List<String> rowKeys, T bean) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> fetch(String rowKey, T bean) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> fetchByPartialKey(List<String> rowKeys, T bean) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> fetchByPartialKey(List<String> rowKeys, T bean, long maxResults) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> fetchByPartialKey(String rowKey, T bean) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> fetchByPartialKey(String rowKey, T bean, long maxResults) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean insert(List<T> rows) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean insert(T row) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> search(List<T> rows) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> search(List<T> rows, long maxResults) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> search(T row) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> search(T row, long maxResults) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> search(T startRow, T endRow) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> search(T startRow, T endRow, long maxResults) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean update(List<T> newRows) throws BlackBoxException {
        MongoInsertTask task = new MongoInsertTask(getCollection(), newRows, false);
        return task.invoke();
    }

    @Override
    public boolean update(T newRow) throws BlackBoxException {
        return update(Arrays.asList(newRow));
    }

    public MongoCollection getCollection() {
        // TODO: make this configurable
        return MongoDBConnectionManager.getMongoDBCollection(null, "dydabo", "dydabo");
    }
}
