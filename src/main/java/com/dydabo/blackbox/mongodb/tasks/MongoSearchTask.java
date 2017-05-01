/*
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;

import static com.mongodb.client.model.Filters.*;

/**
 * @param <T>
 * @author viswadas leher <vleher@gmail.com>
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
        this.utils = new MongoUtils<T>();
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
        for (Map.Entry<String, GenericDBTableRow.ColumnFamily> entry : tableRow.getColumnFamilies().entrySet()) {
            GenericDBTableRow.ColumnFamily colFam = entry.getValue();
            for (Map.Entry<String, GenericDBTableRow.Column> column : colFam.getColumns().entrySet()) {
                String colName = column.getKey();
                GenericDBTableRow.Column colValue = column.getValue();
                if (colValue != null && colValue.getColumnValue() != null) {
                    final String colString = colValue.getColumnValueAsString();
                    if (DyDaBoUtils.isValidRegex(colString)) {
                        if (DyDaBoUtils.isNumber(colValue)) {
                            filterList.add(eq(colName, colValue.getColumnValue()));
                        } else {
                            filterList.add(regex(colName, colString));
                        }
                    }
                }
            }
        }
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
            ForkJoinTask<List<T>> fjTask = new MongoSearchTask<T>(collection, Collections.singletonList(row), maxResult).fork();
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

        Block<Document> addToResultBlock = (Document doc) -> {
            logger.info("Mongo Search Result :" + doc.toJson());
            T resultObject = new Gson().fromJson(doc.toJson(), (Class<T>) row.getClass());
            if (resultObject != null) {
                results.add(resultObject);
            }
        };

        GenericDBTableRow tableRow = utils.convertRowToTableRow(row);

        List<Bson> filterList = parseFilters(tableRow);
        logger.info("Mongo Filter:" + filterList);
        if (filterList.size() > 0) {
            collection.find(and(filterList)).forEach(addToResultBlock);
        } else {
            collection.find().forEach(addToResultBlock);
        }

        return results;
    }

}
