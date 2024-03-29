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
package com.dydabo.blackbox.hbase.utils;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.utils.DyDaBoDBUtils;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Hbase specific utility methods
 *
 * @param <T>
 * @author viswadas leher
 */
public class HBaseUtils<T extends BlackBoxable> implements DyDaBoDBUtils<T> {

	private static final SortedSet<TableName> tableCache = new TreeSet<>();
	private final Logger logger = LogManager.getLogger();

	/**
	 * Alter Hbase tables based on the new row class
	 *
	 * @param row the row class
	 * @param connection a active connection to the database
	 * @return true if successful
	 * @throws IOException database error or exception
	 * @throws BlackBoxException com.dydabo.blackbox.blackbox exception if alter fails
	 */
	public synchronized boolean alterTable(T row, Connection connection) throws IOException {
		try (Admin admin = connection.getAdmin()) {
			TableName tableName = getTableName(row);
			TableDescriptorBuilder tableDescriptor = getTableDescriptorBuilder(row, tableName);
			logger.info("Altering table : {}", tableDescriptor);
			admin.modifyTable(tableDescriptor.build());
		}
		return true;
	}

	private TableDescriptorBuilder getTableDescriptorBuilder(T row, TableName tableName) {
		TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
		GenericDBTableRow hTable = convertRowToTableRow(row);
		Collection<ColumnFamilyDescriptor> colFamilies = new ArrayList<>();
		for (GenericDBTableRow.ColumnFamily value : hTable.getColumnFamilies().values()) {
			ColumnFamilyDescriptor dFamily =
					ColumnFamilyDescriptorBuilder.of(value.getFamilyName());
			colFamilies.add(dFamily);
		}
		tableDescriptor.setColumnFamilies(colFamilies);
		return tableDescriptor;
	}

	/**
	 * Checks if the specified row already exists in the table
	 *
	 * @param row the value of row
	 * @param hTable the value of hTable
	 * @return the boolean
	 * @throws java.io.IOException
	 * @throws com.dydabo.blackbox.BlackBoxException
	 */
	public boolean checkIfRowExists(T row, Table hTable) throws IOException, BlackBoxException {
		if (!isValidRowKey(row)) {
			throw new BlackBoxException("Invalid Row Key");
		}
		Get get = new Get(Bytes.toBytes(row.getBBRowKey()));
		Result result = hTable.get(get);
		return !result.isEmpty();
	}

	/**
	 * Create a table based on the row object
	 *
	 * @param row the value of row
	 * @param connection
	 * @return the boolean
	 * @throws java.io.IOException
	 * @throws com.dydabo.blackbox.BlackBoxException
	 */
	public synchronized boolean createTable(T row, Connection connection) throws IOException {
		TableName tableName = getTableName(row);

		if (tableCache.contains(tableName)) {
			return true;
		}

		try (Admin admin = connection.getAdmin()) {
			if (admin.tableExists(tableName)) {
				tableCache.add(tableName);
				return true;
			}

			TableDescriptorBuilder tableDescriptor = getTableDescriptorBuilder(row, tableName);
			admin.createTable(tableDescriptor.build());
			tableCache.add(tableName);

			return true;
		}
	}

	/**
	 * @param thisValue
	 * @return
	 */
	public byte[] getAsByteArray(Object thisValue) {
		byte[] byteArray = null;
		if (thisValue instanceof Double) {
			byteArray = Bytes.toBytes((Double) thisValue);
		} else if (thisValue instanceof Integer) {
			byteArray = Bytes.toBytes((Integer) thisValue);
		} else if (thisValue instanceof Float) {
			byteArray = Bytes.toBytes((Float) thisValue);
		} else if (thisValue instanceof Boolean) {
			byteArray = Bytes.toBytes((Boolean) thisValue);
		} else if (thisValue instanceof Long) {
			byteArray = Bytes.toBytes((Long) thisValue);
		} else if (thisValue instanceof Short) {
			byteArray = Bytes.toBytes((Short) thisValue);
		} else if (thisValue instanceof BigDecimal) {
			byteArray = Bytes.toBytes((BigDecimal) thisValue);
		} else if (thisValue instanceof String) {
			byteArray = Bytes.toBytes((String) thisValue);
		} else if (thisValue instanceof Map || thisValue instanceof List) {
			String jsonString = new Gson().toJson(thisValue);
			byteArray = Bytes.toBytes(jsonString);
		} else if (thisValue instanceof Date) {
			byteArray = Bytes.toBytes(((Date) thisValue).getTime());
		} else if (thisValue instanceof Instant) {
			byteArray = Bytes.toBytes(((Instant) thisValue).toEpochMilli());
		} else {
			logger.debug("Unknown Value : {}", thisValue);
		}
		return byteArray;
	}

	/**
	 * @param row
	 * @return
	 */
	public TableName getTableName(T row) {
		final String fullClassName = row.getClass().toString().substring(6).replaceAll("\\.", "");
		return TableName.valueOf(fullClassName);
	}

	/**
	 * @param row
	 * @return
	 */
	public boolean isValidRowKey(T row) {
		return row != null && !DyDaBoUtils.isBlankOrNull(row.getBBRowKey());
	}

	/**
	 * @param result
	 * @param row
	 * @return
	 */
	public GenericDBTableRow parseResultToHTable(Result result, T row) throws BlackBoxException {
		GenericDBTableRow resultTable = new GenericDBTableRow(Bytes.toString(result.getRow()));
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = result.getNoVersionMap();
		for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> entry : map.entrySet()) {
			String familyName = Bytes.toString(entry.getKey());
			NavigableMap<byte[], byte[]> famColMap = entry.getValue();
			for (Map.Entry<byte[], byte[]> cols : famColMap.entrySet()) {
				String colName = Bytes.toString(cols.getKey());
				if (cols.getValue() != null) {
					try {
						Field f = DyDaBoUtils.getFieldFromType(row.getClass(), colName);
						if (f == null) {
							logger.fatal("Mismatched column name {}", colName);
							throw new BlackBoxException(
									"Error parsing database column: " + colName);
						}
						if (f.getGenericType().equals(Integer.class)) {
							int colValue = Bytes.toInt(cols.getValue());
							resultTable.getColumnFamily(familyName).addColumn(colName, colValue);
						} else if (f.getGenericType().equals(Double.class)) {
							double colValue = Bytes.toDouble(cols.getValue());
							resultTable.getColumnFamily(familyName).addColumn(colName, colValue);
						} else if (f.getGenericType().equals(Boolean.class)) {
							boolean colValue = Bytes.toBoolean(cols.getValue());
							resultTable.getColumnFamily(familyName).addColumn(colName, colValue);
						} else if (f.getGenericType().equals(Long.class)) {
							long colValue = Bytes.toLong(cols.getValue());
							resultTable.getColumnFamily(familyName).addColumn(colName, colValue);
						} else if (f.getGenericType().equals(Short.class)) {
							short colValue = Bytes.toShort(cols.getValue());
							resultTable.getColumnFamily(familyName).addColumn(colName, colValue);
						} else if (f.getGenericType().equals(Float.class)) {
							float colValue = Bytes.toFloat(cols.getValue());
							resultTable.getColumnFamily(familyName).addColumn(colName, colValue);
						} else if (f.getGenericType().equals(BigDecimal.class)) {
							BigDecimal colValue = Bytes.toBigDecimal(cols.getValue());
							resultTable.getColumnFamily(familyName).addColumn(colName, colValue);
						} else if (f.getGenericType().equals(Date.class)) {
							long colValue = Bytes.toLong(cols.getValue());
							Date thisDate = new Date(colValue);
							resultTable.getColumnFamily(familyName).addColumn(colName, thisDate);
						} else if (f.getGenericType().equals(Instant.class)) {
							long colValue = Bytes.toLong(cols.getValue());
							Instant thisInstant = Instant.ofEpochMilli(colValue);
							resultTable.getColumnFamily(familyName).addColumn(colName, thisInstant);
						} else {
							String colValue = Bytes.toString(cols.getValue());
							resultTable.getColumnFamily(familyName).addColumn(colName, colValue);
						}
					} catch (SecurityException ex) {
						logger.error(ex);
					}
				}
			}
		}
		return resultTable;
	}

	/**
	 * @param regexValue
	 * @return
	 */
	public String sanitizeRegex(String regexValue) {
		// TODO: alter regex to search inside lists and arrays
		if (!DyDaBoUtils.isBlankOrNull(regexValue) && regexValue.trim().startsWith("{")
				&& regexValue.trim().endsWith("}")) {
			return null;
		}
		return regexValue;
	}
}
