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
package com.dydabo.blackbox.mongodb.tasks;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.mongodb.utils.MongoUtils;
import com.google.gson.Gson;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 * @param <T>
 */
public class MongoFetchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private static final Logger logger = Logger.getLogger(MongoFetchTask.class.getName());
    private final MongoCollection<Document> collection;
    private final T row;
    private final List<String> rowKeys;
    private final MongoUtils<T> utils;

    /**
     *
     * @param collection
     * @param rowKeys
     * @param row
     */
    public MongoFetchTask(MongoCollection<Document> collection, List<String> rowKeys, T row) {
        this.collection = collection;
        this.rowKeys = rowKeys;
        this.row = row;
        this.utils = new MongoUtils<T>();
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
            ForkJoinTask<List<T>> fjTask = new MongoFetchTask<T>(collection, Arrays.asList(rowKey), row).fork();
            taskList.add(fjTask);
        }

        for (ForkJoinTask<List<T>> forkJoinTask : taskList) {
            fullResult.addAll(forkJoinTask.join());
        }

        return fullResult;
    }

    private List<T> fetch(String rowKey) {
        List<T> results = new ArrayList<>();

        FindIterable<Document> docIter = collection.find(eq(MongoUtils.PRIMARYKEY, rowKey));

        for (Document doc : docIter) {
            T resultObject = new Gson().fromJson(doc.toJson(), (Class<T>) row.getClass());
            if (resultObject != null) {
                results.add(resultObject);
            }
        }

        return results;
    }

}
