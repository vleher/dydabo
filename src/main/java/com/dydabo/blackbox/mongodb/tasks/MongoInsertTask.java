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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.MongoCollection;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.bson.Document;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 * @param <T>
 */
public class MongoInsertTask<T extends BlackBoxable> {

    private static final Logger logger = Logger.getLogger(MongoInsertTask.class.getName());

    private final boolean checkExisting;
    private final MongoCollection collection;
    private final List<T> rows;
    private final MongoUtils<T> utils;

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

    /**
     *
     * @return
     */
    public Boolean invoke() {
        List<Document> documents = new ArrayList<>();
        for (T row : rows) {
            Document doc = utils.parseRowToDocument(row);
            documents.add(doc);
        }

        collection.insertMany(documents);
        // TODO: do a check existing before inserting...
//        if (checkExisting) {
//            collection.insertMany(documents, insertCallBack);
//        } else {
//            for (Document document : documents) {
//                // TODO: should we do this recusrively/concurrently?
//                collection.replaceOne(eq(MongoUtils.PRIMARYKEY, document.get(MongoUtils.PRIMARYKEY)), document, insertCallBack);
//            }
//        }
        return true;
    }

    private SingleResultCallback insertCallBack = (SingleResultCallback<Void>) (Void t, Throwable thrwbl) -> {
        if (thrwbl != null) {
            // ignore for now  TODO: handle this gracefully
            logger.severe(thrwbl.getMessage());
        }
    };
}
