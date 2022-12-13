/*
 * Copyright 2020 viswadas leher <vleher@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.dydabo.blackbox.mongodb.tasks;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.MaxResultList;
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
import java.util.List;
import java.util.function.Consumer;

/**
 * @author viswadas leher
 */
public class MongoRangeSearchTask<T extends BlackBoxable> extends MongoBaseTask<T, List<T>> {

	private final Logger logger = LogManager.getLogger();

	private final T endRow;
	private final T startRow;
	private final boolean isFirst;
	private final int maxResults;

	public MongoRangeSearchTask(MongoCollection<Document> collection, T startRow, T endRow,
			int maxResults, boolean isFirst) {
		super(collection);
		this.startRow = startRow;
		this.endRow = endRow;
		this.maxResults = maxResults;
		this.isFirst = isFirst;
	}

	@Override
	protected List<T> compute() {
		List<T> results = new MaxResultList<>(maxResults);

		GenericDBTableRow startTableRow = getUtils().convertRowToTableRow(startRow);
		GenericDBTableRow endTableRow = getUtils().convertRowToTableRow(endRow);
		List<Bson> filterList = new ArrayList<>();

		filterList.addAll(getFilterList(startTableRow));
		filterList.addAll(getFilterList(endTableRow));

		String type = startRow.getClass().getTypeName();

		Consumer<Document> addToResultBlock = (Document doc) -> {
			T resultObject = new Gson().fromJson(doc.toJson(), (Type) startRow.getClass());
			if (resultObject != null) {
				if (isFirst && maxResults > 0 && results.size() >= maxResults) {
					logger.debug("Skipping result");
				} else {
					results.add(resultObject);
				}
			}
		};

		logger.debug("Filters :" + filterList);
		filterList.add(Filters.regex(MongoUtils.PRIMARYKEY, type + ":.*"));
		if (filterList.size() > 0) {
			getCollection().find(Filters.and(filterList)).forEach(addToResultBlock);
		} else {
			getCollection().find().forEach(addToResultBlock);
		}

		return results;
	}

	private List<Bson> getFilterList(GenericDBTableRow startTableRow) {
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
		return filterList;
	}
}
