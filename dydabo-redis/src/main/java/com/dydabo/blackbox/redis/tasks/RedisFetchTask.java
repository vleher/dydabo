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
package com.dydabo.blackbox.redis.tasks;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.MaxResultList;
import com.dydabo.blackbox.common.utils.DyDaBoConstants;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.redis.db.RedisConnectionManager;
import com.google.gson.Gson;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;

/** @author viswadas leher */
public class RedisFetchTask<T extends BlackBoxable> extends RedisBaseTask<List<T>, T> {
	private final Logger logger = LogManager.getLogger();
	private final List<T> rows;
	private final boolean isPartialKey;
	private final int maxResults;
	private final boolean isFirst;

	public RedisFetchTask(RedisConnectionManager connectionManager, List<T> rows,
			boolean isPartialKey, int maxResults, boolean isFirst) {
		setConnectionManager(connectionManager);
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
			try (StatefulRedisConnection<String, String> connection =
					getConnectionManager().getConnection()) {
				for (T row : rows) {
					fullResult.addAll(fetch(row, connection));
				}
			}
			return fullResult;
		}

		List<ForkJoinTask<List<T>>> taskList = new ArrayList<>();
		ForkJoinTask<List<T>> fjTaskOne = new RedisFetchTask<>(getConnectionManager(),
				rows.subList(0, rows.size() / 2), isPartialKey, maxResults, isFirst);
		taskList.add(fjTaskOne);
		ForkJoinTask<List<T>> fjTaskTwo = new RedisFetchTask<>(getConnectionManager(),
				rows.subList(rows.size() / 2, rows.size()), isPartialKey, maxResults, isFirst);
		taskList.add(fjTaskTwo);

		return ForkJoinTask.invokeAll(taskList).stream().map(ForkJoinTask::join)
				.flatMap(Collection::stream).collect(Collectors.toList());
	}

	private List<T> fetch(T row, StatefulRedisConnection<String, String> connection) {
		List<T> fullResults = Collections.synchronizedList(new MaxResultList<>(maxResults));

		if (DyDaBoUtils.isBlankOrNull(row.getBBRowKey())) {
			return fullResults;
		}

		RedisCommands<String, String> redisCommands = connection.sync();

		String rowKey = getRedisUtils().getRowKey(row);
		if (isPartialKey || rowKey.contains("*")) {
			List<String> newKeys = redisCommands.keys(rowKey);
			logger.debug("Fetching keys:{}", newKeys.size());
			for (String newKey : newKeys) {
				String result = redisCommands.get(newKey);
				if (!DyDaBoUtils.isBlankOrNull(result)) {
					T resultObj = new Gson().fromJson(result, (Type) row.getClass());
					fullResults.add(resultObj);
					if (isFirst && maxResults > 0 && fullResults.size() >= maxResults) {
						return fullResults;
					}
				}
			}
		} else {
			logger.debug("Fetching key {}", rowKey);
			String result = redisCommands.get(rowKey);
			if (!DyDaBoUtils.isBlankOrNull(result)) {
				T resultObj = new Gson().fromJson(result, (Type) row.getClass());
				fullResults.add(resultObj);
			}
		}
		logger.debug("Fetched for {} : {}", rowKey, fullResults.size());
		return fullResults;
	}
}
