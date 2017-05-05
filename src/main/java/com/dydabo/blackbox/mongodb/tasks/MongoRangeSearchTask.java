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

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;

import static com.mongodb.client.model.Filters.*;

/**
 * @author viswadas leher <vleher@gmail.com>
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
        for (Map.Entry<String, GenericDBTableRow.ColumnFamily> entry : startTableRow.getColumnFamilies().entrySet()) {
            GenericDBTableRow.ColumnFamily colFam = entry.getValue();
            for (Map.Entry<String, GenericDBTableRow.Column> col : colFam.getColumns().entrySet()) {
                String colName = col.getKey();
                GenericDBTableRow.Column colValue = col.getValue();

                if (colValue != null && colValue.getColumnValue() != null) {
                    final String colString = colValue.getColumnValueAsString();
                    if (DyDaBoUtils.isValidRegex(colString)) {
                        if (DyDaBoUtils.isNumber(colValue.getColumnValue())) {
                            filterList.add(gte(colName, colValue.getColumnValue()));
                        } else {
                            filterList.add(gte(colName, colString));
                        }
                    }
                }
            }
        }

        for (Map.Entry<String, GenericDBTableRow.ColumnFamily> entry : endTableRow.getColumnFamilies().entrySet()) {
            GenericDBTableRow.ColumnFamily colFam = entry.getValue();
            for (Map.Entry<String, GenericDBTableRow.Column> col : colFam.getColumns().entrySet()) {
                String colName = col.getKey();
                GenericDBTableRow.Column colValue = col.getValue();

                if (colValue != null && colValue.getColumnValue() != null) {
                    final String colString = colValue.getColumnValueAsString();
                    if (DyDaBoUtils.isValidRegex(colString)) {
                        if (DyDaBoUtils.isNumber(colValue.getColumnValue())) {
                            filterList.add(lt(colName, colValue.getColumnValue()));
                        } else {
                            filterList.add(lt(colName, colString));
                        }
                    }
                }

            }

        }

        Block<Document> addToResultBlock = (Document doc) -> {
            logger.info("Mongo Range Search Result :" + doc.toJson());
            T resultObject = new Gson().fromJson(doc.toJson(), (Type) startRow.getClass());
            if (resultObject != null) {
                results.add(resultObject);
            }
        };

        logger.info("Filters :" + filterList);
        if (filterList.size() > 0) {
            collection.find(and(filterList)).forEach(addToResultBlock);
        } else {
            collection.find().forEach(addToResultBlock);
        }

        return results;
    }
}
