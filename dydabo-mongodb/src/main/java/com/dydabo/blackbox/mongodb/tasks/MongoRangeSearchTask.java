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
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.blackbox.mongodb.utils.MongoUtils;
import com.google.gson.Gson;
import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;

/**
 * @author viswadas leher
 */
public class MongoRangeSearchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private static final Logger logger = Logger.getLogger(MongoRangeSearchTask.class.getName());

    private final MongoCollection<Document> collection;
    private final T endRow;
    private final long maxResults;
    private final T startRow;
    private final MongoUtils<BlackBoxable> utils;

    public MongoRangeSearchTask(MongoCollection<Document> collection, T startRow, T endRow, long maxResults) {
        this.collection = collection;
        this.startRow = startRow;
        this.endRow = endRow;
        this.maxResults = maxResults;
        this.utils = new MongoUtils<>();
    }

    @Override
    protected List<T> compute() {

        List<T> results = new ArrayList<>();

        GenericDBTableRow startTableRow = utils.convertRowToTableRow(startRow);
        GenericDBTableRow endTableRow = utils.convertRowToTableRow(endRow);
        List<Bson> filterList = new ArrayList<>();


        startTableRow.forEach((familyName, columnName, columnValue, columnValueAsString) -> {
            if (DyDaBoUtils.isValidRegex(columnValueAsString)) {
                if (DyDaBoUtils.isNumber(columnValue)) {
                    filterList.add(Filters.gte(columnName, columnValue));
                } else {
                    if (DyDaBoUtils.isARegex(columnValueAsString)) {
                        filterList.add(Filters.regex(columnName, columnValueAsString));
                    } else {
                        filterList.add(Filters.gte(columnName, columnValueAsString));
                    }
                }
            }
        });

        endTableRow.forEach((familyName, columnName, columnValue, columnValueAsString) -> {
            if (DyDaBoUtils.isValidRegex(columnValueAsString)) {
                if (DyDaBoUtils.isNumber(columnValue)) {
                    filterList.add(Filters.lt(columnName, columnValue));
                } else {
                    if (DyDaBoUtils.isARegex(columnValueAsString)) {
                        filterList.add(Filters.regex(columnName, columnValueAsString));
                    } else {
                        filterList.add(Filters.lt(columnName, columnValueAsString));
                    }
                }
            }
        });

        String type = startRow.getClass().getTypeName();

        Block<Document> addToResultBlock = (Document doc) -> {
            T resultObject = new Gson().fromJson(doc.toJson(), (Type) startRow.getClass());
            if (resultObject != null) {
                results.add(resultObject);
            }
        };

        logger.finest("Filters :" + filterList);
        filterList.add(Filters.regex(MongoUtils.PRIMARYKEY, type + ":.*"));
        if (filterList.size() > 0) {
            collection.find(Filters.and(filterList)).forEach(addToResultBlock);
        } else {
            collection.find().forEach(addToResultBlock);
        }

        return results;
    }
}
