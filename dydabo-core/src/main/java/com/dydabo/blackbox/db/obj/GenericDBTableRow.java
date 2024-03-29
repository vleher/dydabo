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
package com.dydabo.blackbox.db.obj;

import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author viswadas leher
 */
public class GenericDBTableRow {

	// NOTE: The ColumnFamily names should be as small as possible for performance
	/**
	 *
	 */
	private static final String DEFAULT_FAMILY = "D";
	private final Map<String, GenericDBTableRow.ColumnFamily> columnFamilies;
	private String rowKey;

	/**
	 * @param rowKey
	 */
	public GenericDBTableRow(String rowKey) {
		this.rowKey = rowKey;
		columnFamilies = new HashMap<>();
		createFamily(GenericDBTableRow.DEFAULT_FAMILY);
	}

	/**
	 * @param familyName
	 * @return
	 */
	public GenericDBTableRow.ColumnFamily getColumnFamily(String familyName) {
		GenericDBTableRow.ColumnFamily colFamily = getColumnFamilies().get(familyName);
		if (colFamily == null) {
			colFamily = createFamily(familyName);
		}
		return colFamily;
	}

	/**
	 * @return
	 */
	public GenericDBTableRow.ColumnFamily getDefaultFamily() {
		return createFamily(GenericDBTableRow.DEFAULT_FAMILY);
	}

	/**
	 * @param familyName
	 * @return
	 */
	public GenericDBTableRow.ColumnFamily createFamily(String familyName) {
		getColumnFamilies().computeIfAbsent(familyName, GenericDBTableRow.ColumnFamily::new);
		return getColumnFamilies().get(familyName);
	}

	/**
	 * @return
	 */
	public String getRowKey() {
		return rowKey;
	}

	/**
	 * @param rowKey
	 */
	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	/**
	 * @return
	 */
	public Map<String, GenericDBTableRow.ColumnFamily> getColumnFamilies() {
		return columnFamilies;
	}

	/**
	 * @return
	 */
	public JsonObject toJsonObject() {
		JsonObject jsonObject = getDefaultFamily().toJsonObject();

		for (GenericDBTableRow.ColumnFamily columnFamily : getColumnFamilies().values()) {
			if (!GenericDBTableRow.DEFAULT_FAMILY.equals(columnFamily.getFamilyName())) {
				jsonObject.add(columnFamily.getFamilyName(), columnFamily.toJsonObject());
			}
		}

		return jsonObject;
	}

	public void forEach(DBTableIterator dbTableIterator) {
		getColumnFamilies().forEach((familyName, columnFamily) -> columnFamily.getColumns()
				.forEach((columnName, column) -> dbTableIterator.accept(familyName, columnName,
						column.getColumnValue(), column.getColumnValueAsString())));
	}

	@Override
	public String toString() {
		return "HBaseTable{" + "rowKey=" + rowKey + ", columnFamilies=" + columnFamilies + '}';
	}

	/**
	 * Collection of columns. This is the child of table rows and contains atleast one column
	 * family.
	 */
	public class ColumnFamily {

		private final Map<String, GenericDBTableRow.Column> columns;
		private String familyName;

		/**
		 * @param familyName
		 */
		public ColumnFamily(String familyName) {
			this.familyName = familyName;
			columns = new HashMap<>();
		}

		/**
		 * @param columnName
		 * @param columnValue
		 */
		public void addColumn(String columnName, Object columnValue) {
			getColumns().put(columnName, new GenericDBTableRow.Column(columnName, columnValue));
		}

		/**
		 * @param colName
		 * @return
		 */
		public GenericDBTableRow.Column getColumn(String colName) {
			return getColumns().get(colName);
		}

		/**
		 * @return
		 */
		public String getFamilyName() {
			return familyName;
		}

		/**
		 * @param familyName
		 */
		public void setFamilyName(String familyName) {
			this.familyName = familyName;
		}

		/**
		 * @return
		 */
		public Map<String, GenericDBTableRow.Column> getColumns() {
			return columns;
		}

		/**
		 * @return
		 */
		public JsonObject toJsonObject() {
			JsonObject jsonObject = new JsonObject();
			for (GenericDBTableRow.Column coln : getColumns().values()) {
				String keyName = coln.getColumnName();
				String value = coln.getColumnValueAsString();
				if (!DyDaBoUtils.isBlankOrNull(value)) {
					JsonElement elem = DyDaBoUtils.parseJsonString(value);
					jsonObject.add(keyName,
							Objects.requireNonNullElseGet(elem, () -> new JsonPrimitive(value)));
				}
			}
			return jsonObject;
		}

		@Override
		public String toString() {
			return "ColumnFamily{" + "familyName=" + familyName + ", column=" + columns + '}';
		}

	}

	/**
	 * Columns inside a column family
	 */
	public class Column {
		private final Logger logger = LogManager.getLogger();

		private String columnName;
		private Object columnValue;

		/**
		 * @param columnName
		 * @param columnValue
		 */
		public Column(String columnName, Object columnValue) {
			this.columnName = columnName;
			this.columnValue = columnValue;
		}

		/**
		 * @return
		 */
		public String getColumnName() {
			return columnName;
		}

		/**
		 * @param columnName
		 */
		public void setColumnName(String columnName) {
			this.columnName = columnName;
		}

		/**
		 * @return
		 */
		public Object getColumnValue() {
			return columnValue;
		}

		/**
		 * @param columnValue
		 */
		public void setColumnValue(Object columnValue) {
			this.columnValue = columnValue;
		}

		/**
		 * @return
		 */
		public String getColumnValueAsString() {
			if (columnValue == null) {
				return null;
			}

			String colStringValue;
			if (DyDaBoUtils.isPrimitiveOrPrimitiveWrapperOrString(columnValue)) {
				if (columnValue instanceof Number) {
					DecimalFormat df = new DecimalFormat("#");
					df.setMaximumFractionDigits(18);
					df.setMaximumIntegerDigits(64);
					colStringValue = df.format(columnValue);
				} else {
					colStringValue = String.valueOf(columnValue);
				}
			} else {
				colStringValue = (new Gson().toJson(columnValue));
			}

			return colStringValue;
		}

		@Override
		public String toString() {
			return "Column{" + "columnName=" + columnName + ", columnValue="
					+ new Gson().toJson(columnValue) + '}';
		}

	}
}
