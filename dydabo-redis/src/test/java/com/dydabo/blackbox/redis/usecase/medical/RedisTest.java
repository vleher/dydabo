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

package com.dydabo.blackbox.redis.usecase.medical;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.dydabo.blackbox.redis.RedisBlackBox;
import com.dydabo.blackbox.redis.db.RedisConnectionManager;
import com.dydabo.test.blackbox.BlackBoxFactory;
import com.dydabo.test.blackbox.usecase.company.SimpleUseCase;

/**
 * @author viswadas leher
 */
public class RedisTest extends SimpleUseCase {

	private RedisConnectionManager connectionManager;

	/**
	 * @throws IOException
	 */
	public RedisTest() throws IOException {
		if (utils.dbToTest.contains(BlackBoxFactory.Databases.REDIS)) {
			connectionManager = new RedisConnectionManager("localhost", 6379, 1);
		}
	}

	@Test
	public void testInsert() {
		final int testSize = 1000;

		if (connectionManager != null) {
			insertionTest(testSize, new RedisBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testUpdate() {
		final int testSize = 1000;

		if (connectionManager != null) {
			updateTest(testSize, new RedisBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testDelete() {
		final int testSize = 1000;
		if (connectionManager != null) {
			deleteTest(testSize, new RedisBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testFetchByPartialKey() {
		final int testSize = 200;
		if (connectionManager != null) {
			fetchPartialKey(testSize, new RedisBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testSearchByName() {
		final int testSize = 5;
		if (connectionManager != null) {
			searchTestByName(testSize, new RedisBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testSearchMultipleTypes() {
		final int testSize = 200;
		if (connectionManager != null) {
			searchMultipleTypes(testSize, new RedisBlackBox<>(connectionManager),
					new RedisBlackBox<>(connectionManager), new RedisBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testSearchWithWildCards() {
		final int testSize = 200;
		if (connectionManager != null) {
			searchWithWildCards(testSize, new RedisBlackBox<>(connectionManager),
					new RedisBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testSearchWithDouble() {
		final int testSize = 200;
		if (connectionManager != null) {
			searchWithDouble(testSize, new RedisBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testRangeSearchDouble() {
		final int testSize = 200;
		if (connectionManager != null) {
			rangeSearchDouble(testSize, new RedisBlackBox<>(connectionManager));
		}
	}
}
