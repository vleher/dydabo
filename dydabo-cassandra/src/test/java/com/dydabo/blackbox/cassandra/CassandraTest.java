/*

* Copyright 2017 viswadas leher <vleher@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.dydabo.blackbox.cassandra;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import com.dydabo.test.blackbox.BlackBoxFactory;
import com.dydabo.test.blackbox.usecase.company.SimpleUseCase;

/**
 * @author viswadas leher
 */
public class CassandraTest extends SimpleUseCase {

	private CassandraConnectionManager cassandraConnectionManager;

	/**
	 * @throws IOException
	 */
	public CassandraTest() throws IOException {
		if (utils.dbToTest.contains(BlackBoxFactory.Databases.CASSANDRA)) {
			cassandraConnectionManager = new CassandraConnectionManager("localhost", 9042);

		}
	}

	@Test
	public void testInsert() {
		final int testSize = 10;

		if (cassandraConnectionManager != null) {
			insertionTest(testSize, new CassandraBlackBox<>(cassandraConnectionManager));
		}
	}

	@Test
	public void testUpdate() {
		final int testSize = 10;

		if (cassandraConnectionManager != null) {
			updateTest(testSize, new CassandraBlackBox<>(cassandraConnectionManager));
		}
	}

	@Test
	public void testDelete() {
		final int testSize = 10;
		if (cassandraConnectionManager != null) {
			deleteTest(testSize, new CassandraBlackBox<>(cassandraConnectionManager));
		}
	}

	@Test
	public void testFetchByPartialKey() {
		final int testSize = 2;
		if (cassandraConnectionManager != null) {
			fetchPartialKey(testSize, new CassandraBlackBox<>(cassandraConnectionManager));
		}
	}

	@Test
	public void testSearchByName() {
		final int testSize = 5;
		if (cassandraConnectionManager != null) {
			searchTestByName(testSize, new CassandraBlackBox<>(cassandraConnectionManager));
		}
	}

	@Test
	public void testSearchMultipleTypes() {
		final int testSize = 2;
		if (cassandraConnectionManager != null) {
			searchMultipleTypes(testSize, new CassandraBlackBox<>(cassandraConnectionManager),
					new CassandraBlackBox<>(cassandraConnectionManager),
					new CassandraBlackBox<>(cassandraConnectionManager));
		}
	}

	@Test
	public void testSearchWithWildCards() {
		final int testSize = 2;
		if (cassandraConnectionManager != null) {
			searchWithWildCards(testSize, new CassandraBlackBox<>(cassandraConnectionManager),
					new CassandraBlackBox<>(cassandraConnectionManager));
		}
	}

	@Test
	public void testSearchWithDouble() {
		final int testSize = 2;
		if (cassandraConnectionManager != null) {
			searchWithDouble(testSize, new CassandraBlackBox<>(cassandraConnectionManager));
		}
	}

	@Test
	public void testRangeSearchDouble() {
		final int testSize = 2;
		if (cassandraConnectionManager != null) {
			rangeSearchDouble(testSize, new CassandraBlackBox<>(cassandraConnectionManager));
		}
	}

}
