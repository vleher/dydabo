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
package com.dydabo.blackbox.hbase.tasks.impl;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.MaxResultList;
import com.dydabo.blackbox.common.utils.DyDaBoConstants;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.blackbox.hbase.tasks.HBaseTask;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @param <T>
 * @author viswadas leher
 */
public class HBaseSearchTask<T extends BlackBoxable> extends HBaseTask<T, List<T>> {

	private final Logger logger = LogManager.getLogger();
	private final List<T> rows;
	private final int maxResults;
	private final boolean isFirst;

	/**
	 * @param connection
	 * @param row
	 * @param maxResults
	 */
	public HBaseSearchTask(Connection connection, T row, int maxResults, boolean isFirst) {
		this(connection, Collections.singletonList(row), maxResults, isFirst);
	}

	/**
	 * @param connection
	 * @param rows
	 * @param maxResults
	 */
	public HBaseSearchTask(Connection connection, List<T> rows, int maxResults, boolean isFirst) {
		super(connection);
		this.rows = rows;
		this.maxResults = maxResults;
		this.isFirst = isFirst;
	}

	@Override
	protected List<T> compute() {
		try {
			return search(rows);
		} catch (BlackBoxException ex) {
			logger.error(ex);
		}
		return new ArrayList<>();
	}

	/**
	 * @param thisTable
	 * @param filterList
	 * @return
	 */
	private boolean parseForFilters(GenericDBTableRow thisTable, FilterList filterList) {
		final AtomicBoolean hasFilters = new AtomicBoolean(false);

		thisTable.forEach((familyName, columnName, columnValue, columnValueAsString) -> {
			String regexValue = utils.sanitizeRegex(columnValueAsString);
			if (regexValue != null && DyDaBoUtils.isValidRegex(regexValue)) {
				if (DyDaBoUtils.isNumber(columnValue)) {
					BinaryComparator regexComp =
							new BinaryComparator(utils.getAsByteArray(columnValue));
					SingleColumnValueFilter scvf =
							new SingleColumnValueFilter(Bytes.toBytes(familyName),
									Bytes.toBytes(columnName), CompareOperator.EQUAL, regexComp);
					scvf.setFilterIfMissing(true);
					filterList.addFilter(scvf);
				} else {
					RegexStringComparator regexComp = new RegexStringComparator(regexValue);
					SingleColumnValueFilter scvf =
							new SingleColumnValueFilter(Bytes.toBytes(familyName),
									Bytes.toBytes(columnName), CompareOperator.EQUAL, regexComp);
					scvf.setFilterIfMissing(true);
					filterList.addFilter(scvf);
				}
				hasFilters.set(true);
			}
		});

		return hasFilters.get();
	}

	/**
	 * @param rows
	 * @return
	 * @throws BlackBoxException
	 */
	private List<T> search(List<T> rows) throws BlackBoxException {

		if (rows.size() < DyDaBoConstants.MIN_PARALLEL_THRESHOLD) {
			List<T> fullResult = new ArrayList<>();
			for (T row : rows) {
				fullResult.addAll(search(row));
			}
			return fullResult;
		} else {
			// create a task for each element or row in the list
			List<ForkJoinTask<List<T>>> taskList = new ArrayList<>();
			for (T row : rows) {
				ForkJoinTask<List<T>> fjTask = new HBaseSearchTask<>(getConnection(),
						Collections.singletonList(row), maxResults, isFirst);
				taskList.add(fjTask);
			}
			return invokeAll(taskList).stream().map(ForkJoinTask::join).flatMap(ts -> ts.stream())
					.collect(Collectors.toList());
		}
	}

	/**
	 * @param row
	 * @return
	 * @throws BlackBoxException
	 */
	private List<T> search(T row) throws BlackBoxException {
		List<T> results = new MaxResultList<>(maxResults);

		try (Table hTable = getConnection().getTable(utils.getTableName(row))) {
			Scan scan = new Scan();

			// Get the filters : just simple regex filters for now
			GenericDBTableRow thisTable = utils.convertRowToTableRow(row);
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			boolean hasFilters = parseForFilters(thisTable, filterList);
			if (hasFilters) {
				logger.debug("Filters: {}", filterList);
				scan.setFilter(filterList);
			}

			try (ResultScanner resultScanner = hTable.getScanner(scan)) {
				for (Result result : resultScanner) {
					GenericDBTableRow resultTable = utils.parseResultToHTable(result, row);

					T resultObject =
							new Gson().fromJson(resultTable.toJsonObject(), (Type) row.getClass());
					results.add(resultObject);
					if (isFirst && results.size() >= maxResults) {
						break;
					}
				}
			}
		} catch (IOException ex) {
			logger.error(ex);
		}
		return results;
	}
}
