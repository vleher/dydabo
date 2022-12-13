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
package com.dydabo.blackbox.hbase.db;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author viswadas leher
 */
public class HBaseConnectionManager {

	private static final int MIN_NUM_CONNECTION = 15;
	private static final Queue<Connection> connectionPool = new ConcurrentLinkedQueue<>();
	private final Logger logger = LogManager.getLogger();
	private final Configuration configuration;

	public HBaseConnectionManager(final Configuration configuration) {
		this.configuration = configuration;
	}

	private void populatePool() {
		final AtomicInteger tries = new AtomicInteger(0);
		while ((connectionPool.size() < (2 * MIN_NUM_CONNECTION))
				&& (tries.getAndIncrement() < (2 * MIN_NUM_CONNECTION))) {
			try {
				connectionPool.add(ConnectionFactory.createConnection(configuration));
			} catch (final Exception e) {
				logger.catching(e);
			}
		}
		logger.debug("Connection pool populated: {}", connectionPool.size());
	}

	public Connection getConnection() throws IOException {
		Connection connection = connectionPool.poll();
		while ((connection != null) && connection.isClosed()) {
			connection = connectionPool.poll();
		}
		if (connectionPool.size() < MIN_NUM_CONNECTION) {
			new Thread(this::populatePool).start();
		}
		if ((connection != null) && !connection.isClosed()) {
			return connection;
		}

		return ConnectionFactory.createConnection(configuration);
	}
}
