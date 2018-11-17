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
    private final List<T> rows;
    private final MongoUtils<T> utils;

    /**
     * @param collection
     * @param rows
     */
    public MongoFetchTask(MongoCollection<Document> collection, List<T> rows, boolean isPartialKey, long maxResults) {
        this.collection = collection;
        this.rows = rows;
        this.utils = new MongoUtils<>();
        this.isPartialKey = isPartialKey;
        this.maxResults = maxResults;
    }

    @Override
    protected List<T> compute() {
        return fetch(rows);
    }

    private List<T> fetch(List<T> rows) {
        if (rows.size() < 2) {
            List<T> fullResult = new ArrayList<>();
            for (T row : rows) {
                fullResult.addAll(fetch(row));
            }
            return fullResult;
        }

        List<T> fullResult = new ArrayList<>();

        List<ForkJoinTask<List<T>>> taskList = new ArrayList<>();
        for (T row : rows) {
            ForkJoinTask<List<T>> fjTask = new MongoFetchTask<T>(collection, Collections.singletonList(row), isPartialKey, maxResults).fork();
            taskList.add(fjTask);
        }

        for (ForkJoinTask<List<T>> forkJoinTask : taskList) {
            fullResult.addAll(forkJoinTask.join());
        }

        return fullResult;
    }

    private List<T> fetch(T row) {
        List<T> results = new ArrayList<>();

        if (DyDaBoUtils.isBlankOrNull(row.getBBRowKey())) {
            return results;
        }

        FindIterable<Document> docIter;

        String rowKey = (row.getClass().getTypeName()) + ":" + row;
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
