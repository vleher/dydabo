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

package com.dydabo.blackbox.redis;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.AbstractBlackBoxImpl;
import com.dydabo.blackbox.redis.db.RedisConnectionManager;
import com.dydabo.blackbox.redis.tasks.RedisDeleteTask;
import com.dydabo.blackbox.redis.tasks.RedisFetchTask;
import com.dydabo.blackbox.redis.tasks.RedisInsertTask;
import com.dydabo.blackbox.redis.tasks.RedisRangeSearchTask;
import com.dydabo.blackbox.redis.tasks.RedisSearchTask;

import java.util.List;

/**
 * @author viswadas leher
 */
public class RedisBlackBox<T extends BlackBoxable> extends AbstractBlackBoxImpl<T>
		implements BlackBox<T> {

	private final RedisConnectionManager connectionManager;

	public RedisBlackBox(RedisConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}

	@Override
	public boolean delete(List<T> rows) throws BlackBoxException {
		RedisDeleteTask<T> delJob = new RedisDeleteTask<>(getConnectionManager(), rows);
		return getForkJoinPool().invoke(delJob);
	}

	@Override
	public List<T> fetch(List<T> rows) throws BlackBoxException {
		RedisFetchTask<T> fetchJob =
				new RedisFetchTask<>(getConnectionManager(), rows, false, Integer.MAX_VALUE, false);
		return getForkJoinPool().invoke(fetchJob);
	}

	@Override
	public List<T> fetchByPartialKey(List<T> rows, int maxResults, boolean isFirst)
			throws BlackBoxException {
		RedisFetchTask<T> fetchTask =
				new RedisFetchTask<>(getConnectionManager(), rows, true, maxResults, isFirst);
		return getForkJoinPool().invoke(fetchTask);
	}

	@Override
	public boolean insert(List<T> rows) throws BlackBoxException {
		RedisInsertTask<T> insertTask = new RedisInsertTask<>(getConnectionManager(), rows, true);
		return getForkJoinPool().invoke(insertTask);
	}

	@Override
	public List<T> search(List<T> rows, int maxResults, boolean isFirst) throws BlackBoxException {
		RedisSearchTask<T> searchTask =
				new RedisSearchTask<>(getConnectionManager(), rows, maxResults, isFirst);
		return getForkJoinPool().invoke(searchTask);
	}

	@Override
	public List<T> search(T startRow, T endRow, int maxResults, boolean isFirst)
			throws BlackBoxException {
		RedisRangeSearchTask<T> rangeTask = new RedisRangeSearchTask<>(getConnectionManager(),
				startRow, endRow, maxResults, isFirst);
		return getForkJoinPool().invoke(rangeTask);
	}

	@Override
	public boolean update(List<T> newRows) throws BlackBoxException {
		RedisInsertTask<T> updateTask =
				new RedisInsertTask<>(getConnectionManager(), newRows, false);
		return getForkJoinPool().invoke(updateTask);
	}

	public RedisConnectionManager getConnectionManager() {
		return connectionManager;
	}
}
