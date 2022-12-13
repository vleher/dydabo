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
import com.dydabo.blackbox.redis.db.RedisConnectionManager;
import com.dydabo.blackbox.redis.utils.RedisUtils;

import java.util.concurrent.RecursiveTask;

public abstract class RedisBaseTask<T, R extends BlackBoxable> extends RecursiveTask<T> {

	private RedisUtils<R> redisUtils = new RedisUtils<>();
	private RedisConnectionManager connectionManager;

	public RedisConnectionManager getConnectionManager() {
		return connectionManager;
	}

	public void setConnectionManager(RedisConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}

	public RedisUtils<R> getRedisUtils() {
		return redisUtils;
	}
}
