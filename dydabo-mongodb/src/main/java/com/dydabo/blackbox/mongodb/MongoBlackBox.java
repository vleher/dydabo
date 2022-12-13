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
package com.dydabo.blackbox.mongodb;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.AbstractBlackBoxImpl;
import com.dydabo.blackbox.mongodb.db.MongoDBConnectionManager;
import com.dydabo.blackbox.mongodb.tasks.MongoDeleteTask;
import com.dydabo.blackbox.mongodb.tasks.MongoFetchTask;
import com.dydabo.blackbox.mongodb.tasks.MongoInsertTask;
import com.dydabo.blackbox.mongodb.tasks.MongoRangeSearchTask;
import com.dydabo.blackbox.mongodb.tasks.MongoSearchTask;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.List;

/**
 * @param <T>
 * @author viswadas leher
 */
public class MongoBlackBox<T extends BlackBoxable> extends AbstractBlackBoxImpl<T>
		implements BlackBox<T> {

	public final MongoDBConnectionManager connectionManager;

	public MongoBlackBox(MongoDBConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}

	@Override
	public boolean delete(List<T> rows) throws BlackBoxException {
		MongoDeleteTask<T> task = new MongoDeleteTask<>(getCollection(), rows);
		return task.invoke();
	}

	@Override
	public List<T> fetch(List<T> rows) throws BlackBoxException {
		MongoFetchTask<T> task =
				new MongoFetchTask<>(getCollection(), rows, false, Integer.MAX_VALUE, false);
		return task.invoke();
	}

	@Override
	public List<T> fetchByPartialKey(List<T> rows, int maxResults, boolean isFirst)
			throws BlackBoxException {
		MongoFetchTask<T> task =
				new MongoFetchTask<>(getCollection(), rows, true, maxResults, isFirst);
		return task.invoke();
	}

	@Override
	public boolean insert(List<T> rows) throws BlackBoxException {
		MongoInsertTask<T> task = new MongoInsertTask<>(getCollection(), rows, true);
		return task.invoke();
	}

	@Override
	public List<T> search(List<T> rows, int maxResults, boolean isFirst) throws BlackBoxException {
		MongoSearchTask<T> task = new MongoSearchTask<>(getCollection(), rows, maxResults, isFirst);
		return task.invoke();
	}

	@Override
	public List<T> search(T startRow, T endRow, int maxResults, boolean isFirst)
			throws BlackBoxException {
		MongoRangeSearchTask<T> task =
				new MongoRangeSearchTask<>(getCollection(), startRow, endRow, maxResults, isFirst);
		return task.invoke();

	}

	@Override
	public boolean update(List<T> newRows) throws BlackBoxException {
		MongoInsertTask<T> task = new MongoInsertTask<>(getCollection(), newRows, false);
		return task.invoke();
	}

	/**
	 * @return
	 */
	private MongoCollection<Document> getCollection() {
		return connectionManager.getMongoDBCollection();
	}
}
