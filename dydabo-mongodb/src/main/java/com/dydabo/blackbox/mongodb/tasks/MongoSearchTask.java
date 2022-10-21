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
import com.dydabo.blackbox.common.MaxResultList;
import com.dydabo.blackbox.common.utils.DyDaBoDBUtils;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.blackbox.mongodb.utils.MongoUtils;
import com.google.gson.Gson;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @param <T>
 * @author viswadas leher
 */
public class MongoSearchTask<T extends BlackBoxable> extends MongoBaseTask<T, List<T>> {

    private final Logger logger = LogManager.getLogger();
    private final int maxResult;
    private final List<T> rows;
    private final boolean isFirst;

    /**
     * @param collection
     * @param rows
     * @param maxResult
     * @param isFirst
     */
    public MongoSearchTask(MongoCollection<Document> collection, List<T> rows, int maxResult, boolean isFirst) {
        super(collection);
        this.rows = rows;
        this.maxResult = maxResult;
        this.isFirst = isFirst;
    }

    @Override
    protected List<T> compute() {
        return search(rows);
    }

    /**
     * @param tableRow the value of tableRow
     * @return
     */
    private List<Bson> parseFilters(GenericDBTableRow tableRow) {
        List<Bson> filterList = new ArrayList<>();

        tableRow.forEach((familyName, columnName, columnValue, columnValueAsString) -> {
            if (DyDaBoUtils.isValidRegex(columnValueAsString)) {
                if (DyDaBoUtils.isNumber(columnValue)) {
                    filterList.add(Filters.eq(columnName, columnValue));
                } else {
                    if (columnValueAsString.startsWith("[") || columnValueAsString.startsWith("{")) {
                        // TODO : search inside maps and arrays
                    } else {
                        filterList.add(Filters.regex(columnName, columnValueAsString));
                    }
                }
            }
        });

        return filterList;
    }

    private List<T> search(List<T> rows) {
        if (rows.size() < DyDaBoDBUtils.MIN_PARALLEL_THRESHOLD) {
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
            ForkJoinTask<List<T>> fjTask = new MongoSearchTask<>(getCollection(), Collections.singletonList(row), maxResult,
                    isFirst);
            taskList.add(fjTask);
        }
        return invokeAll(taskList).stream().map(ForkJoinTask::join).flatMap(ts -> ts.stream()).collect(Collectors.toList());
    }

    private List<T> search(T row) {
        List<T> results = new MaxResultList<>(maxResult);
        String type = row.getClass().getTypeName();
        Consumer<Document> addToResultBlock = (Document doc) -> {
            T resultObject = new Gson().fromJson(doc.toJson(), (Type) row.getClass());
            if (resultObject != null) {
                if (isFirst && maxResult > 0 && results.size() >= maxResult) {
                    logger.debug("Skipping result object.");
                } else {
                    results.add(resultObject);
                }
            }
        };

        GenericDBTableRow tableRow = getUtils().convertRowToTableRow(row);

        List<Bson> filterList = parseFilters(tableRow);
        filterList.add(Filters.regex(MongoUtils.PRIMARYKEY, type + ":.*"));

        logger.debug("Filter: {}", filterList);
        if (filterList.size() > 0) {
            getCollection().find(Filters.and(filterList)).forEach(addToResultBlock);
        }

        return results;
    }
}
