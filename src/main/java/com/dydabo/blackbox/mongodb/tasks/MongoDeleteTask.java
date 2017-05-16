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
import com.dydabo.blackbox.mongodb.utils.MongoUtils;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;

import static com.mongodb.client.model.Filters.eq;

/**
 * @param <T>
 * @author viswadas leher
 */
public class MongoDeleteTask<T extends BlackBoxable> extends RecursiveTask<Boolean> {

    private static final Logger logger = Logger.getLogger(MongoDeleteTask.class.getName());

    private final MongoCollection<Document> collection;
    private final List<T> rows;
    private final MongoUtils<T> utils;

    /**
     * @param collection
     * @param rows
     */
    public MongoDeleteTask(MongoCollection<Document> collection, List<T> rows) {
        this.collection = collection;
        this.rows = rows;
        this.utils = new MongoUtils<>();
    }

    @Override
    protected Boolean compute() {
        // Stop case
        if (rows.size() < 2) {
            boolean successFlag = Boolean.TRUE;
            for (T row : rows) {
                successFlag = successFlag && delete(row);
            }
            return successFlag;
        }

        List<ForkJoinTask<Boolean>> taskList = new ArrayList<>();
        for (T row : rows) {
            ForkJoinTask<Boolean> subTask = new MongoDeleteTask<>(collection, Collections.singletonList(row)).fork();
            taskList.add(subTask);
        }

        boolean successFlag = Boolean.TRUE;
        for (ForkJoinTask<Boolean> fjTask : taskList) {
            successFlag = fjTask.join() && successFlag;
        }
        return successFlag;
    }

    /**
     * @param row
     * @return
     */
    public Boolean delete(T row) {

        Document doc = utils.parseRowToDocument(row);
        DeleteResult delResult = collection.deleteOne(eq(MongoUtils.PRIMARYKEY, doc.get(MongoUtils.PRIMARYKEY)));

        return delResult.wasAcknowledged();
    }

}
