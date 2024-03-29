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
package com.dydabo.blackbox.redis.utils;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.utils.DyDaBoDBUtils;

/**
 * @author viswadas leher
 */
public class RedisUtils<T extends BlackBoxable> implements DyDaBoDBUtils<T> {

	public String getRowKey(T row) {
		String key = row.getClass().getSimpleName() + ":" + row.getBBRowKey();
		return key.replaceAll("\\.\\*", "*");
	}

	public String getRowKey(T row, String rowKey) {
		return row.getClass().getSimpleName() + ":" + rowKey;
	}
}
