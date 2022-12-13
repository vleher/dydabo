/*
 *  Copyright 2020 viswadas leher <vleher@gmail.com>.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.dydabo.blackbox.mongodb.tasks;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.utils.DyDaBoConstants;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;

/**
 * @param <T>
 * @author viswadas leher
 */
public class MongoInsertTask<T extends BlackBoxable> extends MongoBaseTask<T, Boolean> {

    private final Logger logger = LogManager.getLogger();

    private final boolean checkExisting;
    private final List<T> rows;

    public MongoInsertTask(MongoCollection<Document> collection, T row, boolean checkExisting) {
        this(collection, Collections.singletonList(row), checkExisting);
    }

    /**
     * @param collection
     * @param rows
     * @param checkExisting
     */
    public MongoInsertTask(MongoCollection<Document> collection, List<T> rows, boolean checkExisting) {
        super(collection);
        this.rows = rows;
        this.checkExisting = checkExisting;
    }

    @Override
    protected Boolean compute() {
        if (checkExisting) {
            List<Document> documents = new ArrayList<>();
            for (T row : rows) {
                Document doc = getUtils().parseRowToDocument(row);
                documents.add(doc);
            }
            // this inserts the document if there are none with a matching id
            getCollection().insertMany(documents);
        } else {
            if (rows.size() < DyDaBoConstants.MIN_PARALLEL_THRESHOLD) {
                boolean successFlag = true;
                for (T row : rows) {
                    successFlag = successFlag && upsertDocument(getUtils().parseRowToDocument(row));
                }
                return successFlag;
            }
            // replace the existing document with this one, or insert a new one
            List<ForkJoinTask<Boolean>> taskList = new ArrayList<>();
            for (T row : rows) {
                ForkJoinTask<Boolean> fjTask = new MongoInsertTask<>(getCollection(), row, checkExisting);
                taskList.add(fjTask);
            }
            return ForkJoinTask.invokeAll(taskList).stream().map(ForkJoinTask::join).reduce(Boolean::logicalAnd).orElse(false);
        }

        return true;
    }

    private Boolean upsertDocument(Document doc) {
        ReplaceOptions ro = new ReplaceOptions().upsert(true);
        getCollection().replaceOne(Filters.eq("_id", doc.get("_id")), doc, ro);
        return true;
    }
}
