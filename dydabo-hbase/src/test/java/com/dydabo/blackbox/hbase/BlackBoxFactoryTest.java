/*
 * Copyright 2017 viswadas leher .
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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.test.blackbox.BlackBoxFactory;

/**
 * @author viswadas leher
 */
public class BlackBoxFactoryTest {

	private final Logger logger = LogManager.getLogger();

	/**
	 * Test of getDatabase method, of class BlackBoxFactory.
	 */
	@Test
	public void testGetDatabase() {
		// Test Hbase
		final BlackBox<BlackBoxable> result = new HBaseBlackBox<>();
		Assertions.assertTrue(result instanceof HBaseBlackBox);
	}

	/**
	 * Test of getHBaseDatabase method, of class BlackBoxFactory.
	 *
	 * @throws java.lang.Exception
	 */
	@Test
	public void testGetHBaseDatabase() throws Exception {
		HBaseConfiguration.create();
		final BlackBox<BlackBoxable> result = new HBaseBlackBox<>();
		Assertions.assertTrue(result instanceof HBaseBlackBox);
	}

	/**
	 * @throws NoSuchMethodException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException
	 */
	@Test
	public void testConstructorIsPrivate()
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		final Constructor<BlackBoxFactory> constructor = BlackBoxFactory.class.getDeclaredConstructor();
		Assertions.assertTrue(Modifier.isPrivate(constructor.getModifiers()));
		constructor.setAccessible(true);
		constructor.newInstance();
	}
}
