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

package com.dydabo.blackbox.hbase.tasks;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Connection;

import java.util.concurrent.RecursiveTask;

/**
 * @author viswadas leher
 */
public abstract class HBaseTask<T extends BlackBoxable, R> extends RecursiveTask<R> {
	protected final Connection connection;
	protected HBaseUtils<T> utils;

	public HBaseTask(Connection connection) {
		this.utils = new HBaseUtils<>();
		this.connection = connection;
	}

	protected HBaseUtils<T> getUtils() {
		if (utils == null) {
			utils = new HBaseUtils<>();
		}
		return utils;
	}

	/**
	 * @return
	 */
	protected Connection getConnection() {
		return connection;
	}
}
