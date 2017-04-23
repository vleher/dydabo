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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;

import static com.mongodb.client.model.Filters.eq;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 * @param <T>
 */
public class MongoInsertTask<T extends BlackBoxable> extends RecursiveTask<Boolean> {

    private static final Logger logger = Logger.getLogger(MongoInsertTask.class.getName());

    private final boolean checkExisting;
    private final MongoCollection collection;
    private final List<T> rows;
    private final MongoUtils<T> utils;

    public MongoInsertTask(MongoCollection collection, T row, boolean checkExisting) {
        this(collection, Collections.singletonList(row), checkExisting);
    }

    /**
     *
     * @param collection
     * @param rows
     * @param checkExisting
     */
    public MongoInsertTask(MongoCollection collection, List<T> rows, boolean checkExisting) {
        this.collection = collection;
        this.rows = rows;
        this.checkExisting = checkExisting;
        this.utils = new MongoUtils<T>();
    }

    @Override
    protected Boolean compute() {
        if (checkExisting) {
            List<Document> documents = new ArrayList<>();
            for (T row : rows) {
                Document doc = utils.parseRowToDocument(row);
                documents.add(doc);
            }
            // this inserts the document if there are none with a matching id
            collection.insertMany(documents);
        } else {
            if (rows.size() < 2) {
                Boolean successFlag = true;
                for (T row : rows) {
                    successFlag = successFlag && upsertDocument(utils.parseRowToDocument(row));
                }
                return successFlag;
            }
            // replace the existing document with this one, or insert a new one
            List<ForkJoinTask<Boolean>> taskList = new ArrayList<>();
            for (T row : rows) {
                ForkJoinTask<Boolean> fjTask = new MongoInsertTask<>(getCollection(), row, checkExisting).fork();
                taskList.add(fjTask);
            }
            boolean successFlag = Boolean.TRUE;
            for (ForkJoinTask<Boolean> fjTask : taskList) {
                successFlag = fjTask.join() && successFlag;
            }
            return successFlag;
        }

        return true;
    }

    public MongoCollection getCollection() {
        return collection;
    }

    private Boolean upsertDocument(Document doc) {
        UpdateOptions uo = new UpdateOptions().upsert(true);
        getCollection().replaceOne(eq("_id", doc.get("_id")), doc, uo);
        return true;
    }

}
