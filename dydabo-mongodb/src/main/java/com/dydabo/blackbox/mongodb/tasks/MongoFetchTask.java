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

import static com.mongodb.client.model.Filters.regex;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.utils.DyDaBoConstants;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.mongodb.utils.MongoUtils;
import com.google.gson.Gson;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;

/**
 * @param <T>
 * @author viswadas leher
 */
public class MongoFetchTask<T extends BlackBoxable> extends MongoBaseTask<T, List<T>> {

	private final boolean isPartialKey;
	private final int maxResults;
	private final List<T> rows;
	private final boolean isFirst;

	/**
	 * @param collection
	 * @param rows
	 */
	public MongoFetchTask(MongoCollection<Document> collection, List<T> rows, boolean isPartialKey,
			int maxResults, boolean isFirst) {
		super(collection);
		this.rows = rows;
		this.isPartialKey = isPartialKey;
		this.maxResults = maxResults;
		this.isFirst = isFirst;
	}

	@Override
	protected List<T> compute() {
		return fetch(rows);
	}

	private List<T> fetch(List<T> rows) {
		if (rows.size() < DyDaBoConstants.MIN_PARALLEL_THRESHOLD) {
			List<T> fullResult = new ArrayList<>();
			for (T row : rows) {
				fullResult.addAll(fetch(row));
			}
			return fullResult;
		}

		List<ForkJoinTask<List<T>>> taskList = new ArrayList<>();
		for (T row : rows) {
			ForkJoinTask<List<T>> fjTask = new MongoFetchTask<>(getCollection(),
					Collections.singletonList(row), isPartialKey, maxResults, isFirst);
			taskList.add(fjTask);
		}
		return invokeAll(taskList).stream().map(ForkJoinTask::join).flatMap(ts -> ts.stream())
				.collect(Collectors.toList());
	}

	private List<T> fetch(T row) {
		List<T> results = new ArrayList<>();

		if (DyDaBoUtils.isBlankOrNull(row.getBBRowKey())) {
			return results;
		}

		FindIterable<Document> docIter;

		String rowKey = getUtils().getRowKey(row);
		if (isPartialKey) {
			docIter = getCollection().find(regex(MongoUtils.PRIMARYKEY, rowKey));
		} else {
			docIter = getCollection().find(Filters.eq(MongoUtils.PRIMARYKEY, rowKey));
		}

		for (Document doc : docIter) {
			T resultObject = new Gson().fromJson(doc.toJson(), (Type) row.getClass());
			if (resultObject != null) {
				if (maxResults <= 0 || results.size() < maxResults) {
					results.add(resultObject);
				} else {
					break;
				}
			}
		}

		return results;
	}
}
