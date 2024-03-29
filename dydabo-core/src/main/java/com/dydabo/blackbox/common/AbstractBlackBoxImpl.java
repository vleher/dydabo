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
package com.dydabo.blackbox.common;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

/**
 * Abstract Implementation of BlackBox
 *
 * @author viswadas leher
 */
public abstract class AbstractBlackBoxImpl<T extends BlackBoxable> implements BlackBox<T> {
	private static final String UPDATE = "update";
	private static final String INSERT = "insert";
	private static final String SEARCH = "search";
	private static final String FETCH_BY_PARTIAL_KEY = "fetchByPartialKey";
	private static final String FETCH = "fetch";
	private static final String DELETE = "delete";

	private final Logger logger = LogManager.getLogger();

	private final ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();

	public ForkJoinPool getForkJoinPool() {
		return forkJoinPool;
	}

	@Override
	public boolean delete(T row) throws BlackBoxException {
		logger.trace(getClass().getName(), DELETE, row.toString());
		return delete(Collections.singletonList(row));
	}

	@Override
	public List<T> fetch(T row) throws BlackBoxException {
		logger.trace(getClass().getName(), FETCH, row.toString());
		return fetch(Collections.singletonList(row));
	}

	@Override
	public List<T> fetchByPartialKey(List<T> rows) throws BlackBoxException {
		logger.trace(getClass().getName(), FETCH_BY_PARTIAL_KEY, rows);
		return fetchByPartialKey(rows, Integer.MAX_VALUE, false);
	}

	@Override
	public List<T> fetchByPartialKey(T row) throws BlackBoxException {
		logger.trace(getClass().getName(), FETCH_BY_PARTIAL_KEY, row);
		return fetchByPartialKey(row, Integer.MAX_VALUE, false);
	}

	@Override
	public List<T> fetchByPartialKey(T row, int maxResults, boolean isFirst)
			throws BlackBoxException {
		logger.trace(getClass().getName(), FETCH_BY_PARTIAL_KEY, row + " :" + maxResults);
		return fetchByPartialKey(Collections.singletonList(row), maxResults, isFirst);
	}

	@Override
	public boolean insert(T row) throws BlackBoxException {
		logger.trace(getClass().getName(), INSERT, row);
		return insert(Collections.singletonList(row));
	}

	@Override
	public List<T> search(List<T> rows) throws BlackBoxException {
		logger.trace(getClass().getName(), SEARCH, rows);
		return search(rows, Integer.MAX_VALUE, false);
	}

	@Override
	public List<T> search(T row) throws BlackBoxException {
		logger.trace(getClass().getName(), SEARCH, row);
		return search(row, Integer.MAX_VALUE, false);
	}

	@Override
	public List<T> search(T row, int maxResults, boolean isFirst) throws BlackBoxException {
		logger.trace(getClass().getName(), SEARCH, row + " :" + maxResults);
		return search(Collections.singletonList(row), maxResults, isFirst);
	}

	@Override
	public List<T> search(T startRow, T endRow) throws BlackBoxException {
		logger.trace(getClass().getName(), SEARCH, startRow + " :" + endRow);
		return search(startRow, endRow, Integer.MAX_VALUE, false);
	}

	@Override
	public boolean update(T newRow) throws BlackBoxException {
		logger.trace(getClass().getName(), UPDATE, newRow);
		return update(Collections.singletonList(newRow));
	}
}
