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

import com.dydabo.blackbox.mongodb.MongoBlackBox;
import com.dydabo.blackbox.mongodb.db.MongoDBConnectionManager;
import com.dydabo.test.blackbox.BlackBoxFactory;
import com.dydabo.test.blackbox.usecase.company.SimpleUseCase;

/**
 * @author viswadas leher
 */
public class MongoDBTest extends SimpleUseCase {

	private MongoDBConnectionManager connectionManager;

	/**
	 * @throws IOException
	 */
	public MongoDBTest() throws IOException {
		if (utils.dbToTest.contains(BlackBoxFactory.Databases.MONGODB)) {
			connectionManager = new MongoDBConnectionManager(null, "dydabo", "dydabo");
		}
	}

	@Test
	public void testInsert() {
		final int testSize = 10;

		if (this.connectionManager != null) {
			insertionTest(testSize, new MongoBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testUpdate() {
		final int testSize = 10;

		if (this.connectionManager != null) {
			updateTest(testSize, new MongoBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testDelete() {
		final int testSize = 10;
		if (this.connectionManager != null) {
			deleteTest(testSize, new MongoBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testFetchByPartialKey() {
		final int testSize = 2;
		if (this.connectionManager != null) {
			fetchPartialKey(testSize, new MongoBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testSearchByName() {
		final int testSize = 5;
		if (this.connectionManager != null) {
			searchTestByName(testSize, new MongoBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testSearchMultipleTypes() {
		final int testSize = 2;
		if (this.connectionManager != null) {
			searchMultipleTypes(testSize, new MongoBlackBox<>(connectionManager),
					new MongoBlackBox<>(connectionManager), new MongoBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testSearchWithWildCards() {
		final int testSize = 2;
		if (connectionManager != null) {
			searchWithWildCards(testSize, new MongoBlackBox<>(connectionManager),
					new MongoBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testSearchWithDouble() {
		final int testSize = 2;
		if (connectionManager != null) {
			searchWithDouble(testSize, new MongoBlackBox<>(connectionManager));
		}
	}

	@Test
	public void testRangeSearchDouble() {
		final int testSize = 2;
		if (connectionManager != null) {
			rangeSearchDouble(testSize, new MongoBlackBox<>(connectionManager));
		}
	}
}
