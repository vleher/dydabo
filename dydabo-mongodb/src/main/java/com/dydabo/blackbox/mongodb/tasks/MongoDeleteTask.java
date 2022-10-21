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
import com.dydabo.blackbox.common.utils.DyDaBoDBUtils;
import com.dydabo.blackbox.mongodb.utils.MongoUtils;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
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
public class MongoDeleteTask<T extends BlackBoxable> extends MongoBaseTask<T, Boolean> {
    private final Logger logger = LogManager.getLogger();
    private final List<T> rows;

    /**
     * @param collection
     * @param rows
     */
    public MongoDeleteTask(MongoCollection<Document> collection, List<T> rows) {
        super(collection);
        this.rows = rows;
    }

    @Override
    protected Boolean compute() {
        // Stop case
        if (rows.size() < DyDaBoDBUtils.MIN_PARALLEL_THRESHOLD) {
            boolean successFlag = Boolean.TRUE;
            for (T row : rows) {
                successFlag = successFlag && delete(row);
            }
            return successFlag;
        }

        List<ForkJoinTask<Boolean>> taskList = new ArrayList<>();
        for (T row : rows) {
            ForkJoinTask<Boolean> subTask = new MongoDeleteTask<>(getCollection(), Collections.singletonList(row));
            taskList.add(subTask);
        }

        return ForkJoinTask.invokeAll(taskList).stream().map(ForkJoinTask::join).reduce(Boolean::logicalAnd).orElse(false);
    }

    /**
     * @param row
     * @return
     */
    private Boolean delete(T row) {

        Document doc = getUtils().parseRowToDocument(row);
        DeleteResult delResult = getCollection().deleteOne(Filters.eq(MongoUtils.PRIMARYKEY, doc.get(MongoUtils.PRIMARYKEY)));

        return delResult.wasAcknowledged();
    }
}
