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
import com.dydabo.blackbox.common.utils.DyDaBoConstants;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.blackbox.hbase.tasks.HBaseTask;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;

/**
 * @param <T>
 * @author viswadas leher
 */
public class HBaseInsertTask<T extends BlackBoxable> extends HBaseTask<T, Boolean> {

	private final Logger logger = LogManager.getLogger();

	private final boolean checkExisting;
	private final List<T> rows;

	/**
	 * @param connection
	 * @param row
	 * @param checkExisting
	 */
	public HBaseInsertTask(Connection connection, T row, boolean checkExisting) {
		this(connection, Collections.singletonList(row), checkExisting);
	}

	/**
	 * @param connection
	 * @param rows
	 * @param checkExisting
	 */
	public HBaseInsertTask(Connection connection, List<T> rows, boolean checkExisting) {
		super(connection);
		this.rows = rows;
		this.checkExisting = checkExisting;
		this.utils = new HBaseUtils<>();
	}

	/**
	 * @param rows
	 * @param checkExisting
	 * @return
	 * @throws BlackBoxException
	 */
	private Boolean insert(List<T> rows, boolean checkExisting) throws BlackBoxException {
		boolean successFlag = Boolean.TRUE;
		if (rows.size() < DyDaBoConstants.MIN_PARALLEL_THRESHOLD) {
			for (T t : rows) {
				successFlag = successFlag && insert(t, checkExisting);
			}
			return successFlag;
		}

		// create a task for each element or row in the list
		List<ForkJoinTask<Boolean>> taskList = new ArrayList<>();
		for (T row : rows) {
			ForkJoinTask<Boolean> fjTask = new HBaseInsertTask<>(getConnection(),
					Collections.singletonList(row), checkExisting);
			taskList.add(fjTask);
		}
		return ForkJoinTask.invokeAll(taskList).stream().map(ForkJoinTask::join)
				.reduce(Boolean::logicalAnd).orElse(false);
	}

	/**
	 * @param row
	 * @param checkExisting
	 * @return
	 * @throws BlackBoxException
	 */
	private Boolean insert(T row, boolean checkExisting) throws BlackBoxException {
		boolean successFlag = true;
		try (final Table hTable = getConnection().getTable(utils.getTableName(row))) {
			// We should "insert" only if there is no such or similar row.
			// If there is one, then the user should call update (not insert).
			if (checkExisting) {
				boolean rowExists = utils.checkIfRowExists(row, hTable);
				if (rowExists) {
					throw new BlackBoxException("Cannot insert as the row exist. "
							+ "Change the rowkey or call update. Current " + "RowKey "
							+ row.getBBRowKey());
				}
			}

			// Find all the fields in the object
			GenericDBTableRow thisTable = utils.convertRowToTableRow(row);
			if (utils.isValidRowKey(row)) {
				Put put = new Put(Bytes.toBytes(row.getBBRowKey()));

				thisTable.forEach((familyName, columnName, columnValue, columnValueAsString) -> {
					byte[] byteArray = utils.getAsByteArray(columnValue);
					if (byteArray != null) {
						put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
								byteArray);
					}
				});

				try {
					logger.debug("Updating the row : {}", put.toString());
					hTable.put(put);
				} catch (NoSuchColumnFamilyException ncfEx) {
					utils.alterTable(row, connection);
					successFlag = false;
				}
			} else {
				successFlag = false;
			}
		} catch (IOException ex) {
			logger.error(ex);
			successFlag = false;
		}
		logger.debug("Inserted Row : {} : {}", row, successFlag);
		return successFlag;
	}

	@Override
	protected Boolean compute() {
		try {
			return insert(rows, checkExisting);
		} catch (BlackBoxException ex) {
			logger.error(ex);
		}
		return false;
	}
}
