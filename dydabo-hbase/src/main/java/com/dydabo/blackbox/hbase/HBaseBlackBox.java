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
package com.dydabo.blackbox.hbase;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.AbstractBlackBoxImpl;
import com.dydabo.blackbox.hbase.db.HBaseConnectionManager;
import com.dydabo.blackbox.hbase.tasks.impl.HBaseDeleteTask;
import com.dydabo.blackbox.hbase.tasks.impl.HBaseFetchTask;
import com.dydabo.blackbox.hbase.tasks.impl.HBaseInsertTask;
import com.dydabo.blackbox.hbase.tasks.impl.HBaseRangeSearchTask;
import com.dydabo.blackbox.hbase.tasks.impl.HBaseSearchTask;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @param <T>
 * @author viswadas leher
 */
public class HBaseBlackBox<T extends BlackBoxable> extends AbstractBlackBoxImpl<T>
		implements BlackBox<T> {

	private final Logger logger = LogManager.getLogger(HBaseBlackBox.class);
	private final HBaseConnectionManager connectionManager;
	private HBaseUtils<T> hBaseUtils = new HBaseUtils<>();

	/**
	 * @throws IOException
	 */
	public HBaseBlackBox() {
		this(new HBaseConnectionManager(HBaseConfiguration.create()));
	}

	/**
	 * @param connectionManager the connection manager
	 * @throws java.io.IOException
	 */
	public HBaseBlackBox(HBaseConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}

	/**
	 * @param rows
	 * @throws BlackBoxException
	 */
	private void createTable(List<T> rows) throws BlackBoxException {
		logger.traceEntry("createtable for: {}", rows.size());
		try (Connection connection = getConnection()) {
			rows.forEach(t -> {
				try {
					hBaseUtils.createTable(t, connection);
				} catch (IOException e) {
					logger.catching(e);
				}
			});
		} catch (IOException ex) {
			logger.error(ex);
			throw new BlackBoxException(ex.getMessage(), ex);
		}
		logger.traceExit("created table");
	}

	@Override
	public boolean delete(List<T> rows) throws BlackBoxException {
		logger.traceEntry("Deleting rows: {}", rows.size());
		createTable(rows);
		try (Connection connection = getConnection()) {
			HBaseDeleteTask<T> deleteJob = new HBaseDeleteTask<>(connection, rows);
			return getForkJoinPool().invoke(deleteJob);
		} catch (IOException ex) {
			logger.error(ex);
		}
		return false;
	}

	@Override
	public List<T> fetch(List<T> rows) throws BlackBoxException {
		logger.traceEntry("fetching rows: {}", rows.size());
		try (Connection connection = getConnection()) {
			HBaseFetchTask<T> fetchTask = new HBaseFetchTask<>(connection, rows, false);
			return getForkJoinPool().invoke(fetchTask);
		} catch (IOException ex) {
			logger.error(ex);
		}
		return new ArrayList<>();
	}

	@Override
	public List<T> fetchByPartialKey(List<T> rows, int maxResults, boolean isFirst)
			throws BlackBoxException {
		logger.traceEntry("partial key fetch: {}", rows.size());
		try (Connection connection = getConnection()) {
			HBaseFetchTask<T> fetchTask =
					new HBaseFetchTask<>(connection, rows, true, maxResults, isFirst);
			return getForkJoinPool().invoke(fetchTask);
		} catch (IOException ex) {
			logger.error(ex);
		}
		return Collections.emptyList();
	}

	@Override
	public boolean insert(List<T> rows) throws BlackBoxException {
		logger.traceEntry("inserting rows: {}", rows.size());
		createTable(rows);
		try (Connection connection = getConnection()) {
			HBaseInsertTask<T> insertJob = new HBaseInsertTask<>(connection, rows, true);
			return getForkJoinPool().invoke(insertJob);
		} catch (IOException ex) {
			logger.error(ex);
		}

		return false;
	}

	@Override
	public List<T> search(List<T> rows, int maxResults, boolean isFirst) throws BlackBoxException {
		logger.traceEntry("searching rows: {}", rows.size());
		createTable(rows);
		try (Connection connection = getConnection()) {
			HBaseSearchTask<T> searchTask =
					new HBaseSearchTask<>(connection, rows, maxResults, isFirst);
			return getForkJoinPool().invoke(searchTask);
		} catch (IOException ex) {
			logger.error(ex);
		}
		return Collections.emptyList();
	}

	@Override
	public List<T> search(T startRow, T endRow, int maxResults, boolean isFirst)
			throws BlackBoxException {
		logger.traceEntry("searching range rows: {}", maxResults);
		createTable(Collections.singletonList(startRow));
		if (startRow.getClass().equals(endRow.getClass())) {
			try (Connection connection = getConnection()) {
				HBaseRangeSearchTask<T> searchTask = new HBaseRangeSearchTask<>(connection,
						startRow, endRow, maxResults, isFirst);
				return getForkJoinPool().invoke(searchTask);
			} catch (IOException ex) {
				logger.error(ex);
			}
		}
		return Collections.emptyList();
	}

	@Override
	public boolean update(List<T> rows) throws BlackBoxException {
		logger.traceEntry("updating rows: {}", rows.size());
		createTable(rows);
		try (Connection connection = getConnection()) {
			HBaseInsertTask<T> insertJob = new HBaseInsertTask<>(connection, rows, false);
			return getForkJoinPool().invoke(insertJob);
		} catch (IOException ex) {
			logger.error(ex);
		}

		return false;
	}

	/**
	 * @return
	 * @throws java.io.IOException
	 */
	private Connection getConnection() throws IOException {
		return connectionManager.getConnection();
	}

	public HBaseUtils<T> gethBaseUtils() {
		return hBaseUtils;
	}

	public void sethBaseUtils(HBaseUtils<T> hBaseUtils) {
		this.hBaseUtils = hBaseUtils;
	}
}
