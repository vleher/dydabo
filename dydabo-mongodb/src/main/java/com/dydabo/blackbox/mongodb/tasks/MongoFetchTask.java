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
package com.dydabo.blackbox.mongodb.tasks;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.mongodb.utils.MongoUtils;
import com.google.gson.Gson;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;

import static com.mongodb.client.model.Filters.regex;

/**
 * @param <T>
 * @author viswadas leher
 */
public class MongoFetchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private static final Logger logger = Logger.getLogger(MongoFetchTask.class.getName());
    private final MongoCollection<Document> collection;
    private final boolean isPartialKey;
    private final long maxResults;
    private final T row;
    private final List<String> rowKeys;
    private final MongoUtils<T> utils;

    /**
     * @param collection
     * @param rowKeys
     * @param row
     */
    public MongoFetchTask(MongoCollection<Document> collection, List<String> rowKeys, T row, boolean isPartialKey, long maxResults) {
        this.collection = collection;
        this.rowKeys = rowKeys;
        this.row = row;
        this.utils = new MongoUtils<>();
        this.isPartialKey = isPartialKey;
        this.maxResults = maxResults;
    }

    @Override
    protected List<T> compute() {
        return fetch(rowKeys);
    }

    private List<T> fetch(List<String> rowKeys) {
        if (rowKeys.size() < 2) {
            List<T> fullResult = new ArrayList<>();
            for (String rowKey : rowKeys) {
                fullResult.addAll(fetch(rowKey));
            }
            return fullResult;
        }

        List<T> fullResult = new ArrayList<>();

        List<ForkJoinTask<List<T>>> taskList = new ArrayList<>();
        for (String rowKey : rowKeys) {
            ForkJoinTask<List<T>> fjTask = new MongoFetchTask<>(collection, Collections.singletonList(rowKey), row, isPartialKey, maxResults).fork();
            taskList.add(fjTask);
        }

        for (ForkJoinTask<List<T>> forkJoinTask : taskList) {
            fullResult.addAll(forkJoinTask.join());
        }

        return fullResult;
    }

    private List<T> fetch(String rowKey) {
        List<T> results = new ArrayList<>();

        if (DyDaBoUtils.isBlankOrNull(rowKey)) {
            return results;
        }

        FindIterable<Document> docIter;

        rowKey = (row.getClass().getTypeName()) + ":" + rowKey;
        if (isPartialKey) {
            docIter = collection.find(regex(MongoUtils.PRIMARYKEY, rowKey));
        } else {
            docIter = collection.find(Filters.eq(MongoUtils.PRIMARYKEY, rowKey));
        }

        for (Document doc : docIter) {
            T resultObject = new Gson().fromJson(doc.toJson(), (Type) row.getClass());
            if (resultObject != null) {
                if (maxResults <= 0 || results.size() < maxResults) {
                    results.add(resultObject);
                } else {
                    break;
                }
            }
        }

        return results;
    }

}
