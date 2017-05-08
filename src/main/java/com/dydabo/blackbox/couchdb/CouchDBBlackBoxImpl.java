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

package com.dydabo.blackbox.couchdb;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;

import java.util.List;

/**
 * Created by leher on 5/5/17.
 */
public class CouchDBBlackBoxImpl<T extends BlackBoxable> implements BlackBox<T> {
    @Override
    public boolean delete(List<T> rows) throws BlackBoxException {
        return false;
    }

    @Override
    public boolean delete(T row) throws BlackBoxException {
        return false;
    }

    @Override
    public List<T> fetch(List<String> rowKeys, T bean) throws BlackBoxException {
        return null;
    }

    @Override
    public List<T> fetch(String rowKey, T bean) throws BlackBoxException {
        return null;
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
        return false;
    }

    @Override
    public boolean update(T newRow) throws BlackBoxException {
        return false;
    }
}
