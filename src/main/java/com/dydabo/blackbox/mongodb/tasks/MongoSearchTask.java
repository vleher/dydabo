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
import com.dydabo.blackbox.common.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.blackbox.mongodb.utils.MongoUtils;
import com.google.gson.Gson;
import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;

import static com.mongodb.client.model.Filters.*;

/**
 * @param <T>
 * @author viswadas leher
 */
public class MongoSearchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private static final Logger logger = Logger.getLogger(MongoSearchTask.class.getName());
    private final MongoCollection<Document> collection;
    private final long maxResult;
    private final List<T> rows;
    private final MongoUtils<T> utils;

    /**
     * @param collection
     * @param rows
     * @param maxResult
     */
    public MongoSearchTask(MongoCollection<Document> collection, List<T> rows, long maxResult) {
        this.collection = collection;
        this.rows = rows;
        this.maxResult = maxResult;
        this.utils = new MongoUtils<>();
    }

    @Override
    protected List<T> compute() {
        return search(rows);
    }

    /**
     * @param tableRow the value of tableRow
     * @return
     */
    protected List<Bson> parseFilters(GenericDBTableRow tableRow) {
        List<Bson> filterList = new ArrayList<>();

        tableRow.forEach((familyName, columnName, columnValue, columnValueAsString) -> {
            if (DyDaBoUtils.isValidRegex(columnValueAsString)) {
                if (DyDaBoUtils.isNumber(columnValue)) {
                    filterList.add(eq(columnName, columnValue));
                } else {
                    if (columnValueAsString.startsWith("[") || columnValueAsString.startsWith("{")) {
                        // TODO : search inside maps and arrays
                    } else {
                        filterList.add(regex(columnName, columnValueAsString));
                    }
                }
            }
        });

        return filterList;
    }

    private List<T> search(List<T> rows) {
        if (rows.size() < 2) {
            List<T> fullResult = new ArrayList<>();
            for (T row : rows) {
                fullResult.addAll(search(row));
            }
            return fullResult;
        }

        List<T> fullResult = new ArrayList<>();

        // create a task for each row
        List<ForkJoinTask<List<T>>> taskList = new ArrayList<>();
        for (T row : rows) {
            ForkJoinTask<List<T>> fjTask = new MongoSearchTask<>(collection, Collections.singletonList(row), maxResult).fork();
            taskList.add(fjTask);
        }

        // wait for all threads to finish
        for (ForkJoinTask<List<T>> fjTask : taskList) {
            fullResult.addAll(fjTask.join());
        }

        return fullResult;
    }

    private List<T> search(T row) {
        List<T> results = new ArrayList<>();
        String type = row.getClass().getTypeName();
        Block<Document> addToResultBlock = (Document doc) -> {
            T resultObject = new Gson().fromJson(doc.toJson(), (Type) row.getClass());
            if (resultObject != null) {
                if (maxResult <= 0) {
                    results.add(resultObject);
                } else if (results.size() < maxResult) {
                    results.add(resultObject);
                }
            }
        };

        GenericDBTableRow tableRow = utils.convertRowToTableRow(row);

        List<Bson> filterList = parseFilters(tableRow);
        filterList.add(regex(MongoUtils.PRIMARYKEY, type + ":.*"));

        logger.finest("Mongo Filter:" + filterList);
        if (filterList.size() > 0) {
            collection.find(and(filterList)).forEach(addToResultBlock);
        }

        return results;
    }

}
