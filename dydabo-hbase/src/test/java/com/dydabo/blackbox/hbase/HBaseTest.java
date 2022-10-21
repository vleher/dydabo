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

package com.dydabo.blackbox.hbase;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.test.blackbox.BlackBoxFactory;
import com.dydabo.test.blackbox.usecase.company.Customer;
import com.dydabo.test.blackbox.usecase.company.SimpleUseCase;

/**
 * @author viswadas leher
 */
public class HBaseTest extends SimpleUseCase {
	public static final int TEST_SIZE = 100;
	protected BlackBox<BlackBoxable> instance;

	/**
	 * @throws IOException
	 */
	public HBaseTest() throws IOException {
		if (utils.dbToTest.contains(BlackBoxFactory.Databases.HBASE)) {
			this.instance = new HBaseBlackBox<>();
		}
	}

	@Test
	public void testInsert() {
		final int testSize = TEST_SIZE;

		if (this.instance != null) {
			insertionTest(testSize, new HBaseBlackBox<Customer>());
		}
	}

	@Test
	public void testUpdate() {
		final int testSize = TEST_SIZE;

		if (this.instance != null) {
			updateTest(testSize, new HBaseBlackBox<>());
		}
	}

	@Test
	public void testDelete() {
		final int testSize = TEST_SIZE;
		if (this.instance != null) {
			deleteTest(testSize, new HBaseBlackBox<>());
		}
	}

	@Test
	public void testFetchByPartialKey() {
		final int testSize = TEST_SIZE;
		if (this.instance != null) {
			fetchPartialKey(testSize, new HBaseBlackBox<>());
		}
	}

	@Test
	public void testSearchByName() {
		final int testSize = TEST_SIZE;
		if (this.instance != null) {
			searchTestByName(testSize, new HBaseBlackBox<>());
		}
	}

	@Test
	public void testSearchMultipleTypes() {
		final int testSize = TEST_SIZE;
		if (this.instance != null) {
			searchMultipleTypes(testSize, new HBaseBlackBox<>(), new HBaseBlackBox<>(), new HBaseBlackBox<>());
		}
	}

	@Test
	public void testSearchWithWildCards() {
		final int testSize = TEST_SIZE;
		if (instance != null) {
			searchWithWildCards(testSize, new HBaseBlackBox<>(), new HBaseBlackBox<>());
		}
	}

	@Test
	public void testSearchWithDouble() {
		final int testSize = TEST_SIZE;
		if (instance != null) {
			searchWithDouble(testSize, new HBaseBlackBox<>());
		}
	}

	@Test
	public void testRangeSearchDouble() {
		final int testSize = TEST_SIZE;
		if (instance != null) {
			rangeSearchDouble(testSize, new HBaseBlackBox<>());
		}
	}
}
