/*
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
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

import java.util.List;

/**
 * @author viswadas leher
 */
public class RedisBlackBoxImpl<T extends BlackBoxable> implements BlackBox<T> {
    @Override
    public boolean delete(List rows) throws BlackBoxException {
        return false;
    }

    @Override
    public boolean delete(BlackBoxable row) throws BlackBoxException {
        return false;
    }

    @Override
    public List fetch(String rowKey, BlackBoxable bean) throws BlackBoxException {
        return null;
    }

    @Override
    public List fetchByPartialKey(String rowKey, BlackBoxable bean) throws BlackBoxException {
        return null;
    }

    @Override
    public List fetchByPartialKey(String rowKey, BlackBoxable bean, long maxResults) throws BlackBoxException {
        return null;
    }

    @Override
    public boolean insert(List rows) throws BlackBoxException {
        return false;
    }

    @Override
    public boolean insert(BlackBoxable row) throws BlackBoxException {
        return false;
    }

    @Override
    public List search(List rows) throws BlackBoxException {
        return null;
    }

    @Override
    public List search(List rows, long maxResults) throws BlackBoxException {
        return null;
    }

    @Override
    public List search(BlackBoxable row) throws BlackBoxException {
        return null;
    }

    @Override
    public List search(BlackBoxable row, long maxResults) throws BlackBoxException {
        return null;
    }

    @Override
    public List search(BlackBoxable startRow, BlackBoxable endRow) throws BlackBoxException {
        return null;
    }

    @Override
    public List search(BlackBoxable startRow, BlackBoxable endRow, long maxResults) throws BlackBoxException {
        return null;
    }

    @Override
    public boolean update(List newRows) throws BlackBoxException {
        return false;
    }

    @Override
    public boolean update(BlackBoxable newRow) throws BlackBoxException {
        return false;
    }

    @Override
    public List fetchByPartialKey(List rowKeys, BlackBoxable bean, long maxResults) throws BlackBoxException {
        return null;
    }

    @Override
    public List fetchByPartialKey(List rowKeys, BlackBoxable bean) throws BlackBoxException {
        return null;
    }

    @Override
    public List fetch(List rowKeys, BlackBoxable bean) throws BlackBoxException {
        return null;
    }
}
