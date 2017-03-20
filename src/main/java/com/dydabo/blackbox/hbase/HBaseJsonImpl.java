/*
 * Copyright (C) 2017 viswadas leher <vleher@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.dydabo.blackbox.hbase;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.DyDaBoUtils;
import com.dydabo.blackbox.db.HBaseConnectionManager;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseJsonImpl<T extends BlackBoxable> implements BlackBox<T> {

    // The ColumnFamily names should be as small as possible for performance
    protected static final String DEFAULT_FAMILY = "D";
    private final Configuration config;

    /**
     *
     */
    public HBaseJsonImpl() throws IOException {
        this.config = HBaseConfiguration.create();
    }

    /**
     *
     * @param config
     */
    public HBaseJsonImpl(Configuration config) throws IOException {
        this.config = config;
    }

    /**
     *
     * @return
     */
    public Connection getConnection() throws IOException {
        return HBaseConnectionManager.getConnection(config);
    }

    public boolean delete(T row) throws BlackBoxException {
        try (Admin admin = getConnection().getAdmin()) {
            // consider create to be is nothing but alter...so
            createTable(row, admin);
            try (Table hTable = admin.getConnection().getTable(getTableName(row))) {
                Delete delete = new Delete(Bytes.toBytes(row.getBBRowKey()));
                delete.addFamily(Bytes.toBytes(DEFAULT_FAMILY));
                hTable.delete(delete);
            }
            return true;
        } catch (IOException ex) {
            Logger.getLogger(HBaseJsonImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    public boolean insert(T row) throws BlackBoxException {
        return insert(row, true);
    }

    protected boolean insert(T row, boolean checkExisting) throws BlackBoxException {
        boolean successFlag = true;
        try (Admin admin = getConnection().getAdmin()) {
            // consider create to be is nothing but alter...so
            createTable(row, admin);

            try (Table hTable = admin.getConnection().getTable(getTableName(row))) {
                // We should "insert" only if there is no such or similar row.
                // If there is one, then the user should call update (not insert).
                if (checkExisting) {
                    boolean rowExists = checkIfRowExists(row, hTable);
                    if (rowExists) {
                        throw new BlackBoxException("Cannot insert as the row exist. "
                                + "Change the rowkey or call update. Current RowKey " + row.getBBRowKey());
                    }
                }
                // Find all the fields in the object
                Put put = new Put(Bytes.toBytes(row.getBBRowKey()));

                Map<String, String> valueMap = new HashMap<>();
                info("Original JSON:" + row.getBBJson());

                convertJsonToMap(row, valueMap);

                info(row.getBBRowKey() + " Mapped :" + valueMap);

                for (String key : valueMap.keySet()) {
                    final String value = valueMap.get(key);
                    put.addColumn(Bytes.toBytes(DEFAULT_FAMILY), Bytes.toBytes(key), Bytes.toBytes(value));
                }

                hTable.put(put);
            }
        } catch (IOException ex) {
            Logger.getLogger(HBaseJsonImpl.class.getName()).log(Level.SEVERE, null, ex);
            successFlag = false;
        }
        return successFlag;
    }

    protected Map<String, String> convertJsonToMap(T row, Map<String, String> valueMap) throws JsonSyntaxException {
        JsonParser jsonParser = new JsonParser();
        JsonElement jsonTree = jsonParser.parse(row.getBBJson());

        for (Map.Entry<String, JsonElement> entry : jsonTree.getAsJsonObject().entrySet()) {
            JsonElement val = entry.getValue();
            info("K:" + entry.getKey() + " => " + val.toString());
            valueMap.put(entry.getKey(), val.toString());
        }

        return valueMap;
    }

    public List<T> select(T row) throws BlackBoxException {
        List<T> results = new ArrayList<>();

        try (Admin admin = getConnection().getAdmin()) {
            // consider create to be is nothing but alter...so
            createTable(row, admin);
            try (Table hTable = admin.getConnection().getTable(getTableName(row))) {
                Scan scan = new Scan();
                // Get the filters
                Map<String, String> valueMap = new HashMap<>();
                valueMap = convertJsonToMap(row, valueMap);

                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                for (Map.Entry<String, String> entry : valueMap.entrySet()) {
                    String columnName = entry.getKey();
                    String regexValue = entry.getValue();
                    if (!DyDaBoUtils.isBlankOrNull(columnName, regexValue)) {
                        RegexStringComparator regexComp = new RegexStringComparator(regexValue);
                        SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes(DEFAULT_FAMILY),
                                Bytes.toBytes(columnName), CompareFilter.CompareOp.EQUAL, regexComp);
                        filterList.addFilter(scvf);
                        info("Filter: " + columnName + " :: " + regexValue + " :" + scvf.toString());
                    }
                }
                info("Filter List:" + filterList);
                scan.setFilter(filterList);

                try (ResultScanner resultScanner = hTable.getScanner(scan)) {
                    for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
                        HashMap<String, String> valueTable = new HashMap<>();

                        for (Cell listCell : result.listCells()) {
                            String value = Bytes.toString(CellUtil.cloneValue(listCell));
                            String keyName = Bytes.toString(CellUtil.cloneQualifier(listCell));
                            valueTable.put(keyName, value);
                        }

                        String jsonString = generateJson(valueTable);
                        info("JSON String:" + jsonString);

                        T resultObject = new Gson().fromJson(jsonString, (Class<T>) row.getClass());
                        results.add(resultObject);
                    }
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(HBaseJsonImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        info("Select Results: " + results);
        return results;
    }

    public boolean update(T row) throws BlackBoxException {
        return insert(row, false);
    }

    @Override
    public boolean delete(List<T> rows) throws BlackBoxException {
        boolean successFlag = true;
        // TODO: do this faster (??)
        for (T t : rows) {
            boolean flag = delete(t);
            successFlag = successFlag && flag;
        }

        return successFlag;
    }

    @Override
    public boolean insert(List<T> rows) throws BlackBoxException {
        boolean successFlag = true;
        // TODO: do this faster (??)
        for (T t : rows) {
            boolean flag = insert(t);
            successFlag = successFlag && flag;
        }

        return successFlag;
    }

    @Override
    public List<T> select(List<T> rows) throws BlackBoxException {
        List<T> combinedResults = new ArrayList<>();
        // TODO: do this faster (??)
        for (T t : rows) {
            List<T> results = select(t);
            combinedResults.addAll(results);
        }
        return combinedResults;
    }

    @Override
    public boolean update(List<T> rows) throws BlackBoxException {
        boolean successFlag = true;
        // TODO: do this faster (??)
        for (T t : rows) {
            boolean flag = update(t);
            successFlag = successFlag && flag;
        }

        return successFlag;
    }

    /**
     *
     * @param row the value of row
     * @param admin the value of admin
     * @return the boolean
     * @throws java.io.IOException
     */
    protected boolean createTable(T row, Admin admin) throws IOException {
        TableName tableName = getTableName(row);
        if (!admin.tableExists(tableName)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor dFamily = new HColumnDescriptor(DEFAULT_FAMILY);
            tableDescriptor.addFamily(dFamily);

            admin.createTable(tableDescriptor);
        }

        return true;
    }

    protected TableName getTableName(T row) {
        final String fullClassName = row.getClass().toString().substring(6).replaceAll("\\.", "");
        return TableName.valueOf(fullClassName);
    }

    private String generateJson(HashMap<String, String> valueTable) {
        String jsonString = new Gson().toJson(valueTable);
        return jsonString;
    }

    private void info(String msg) {
        Logger.getLogger(BlackBox.class.getName()).log(Level.INFO, msg);
    }

    /**
     *
     * @param row the value of row
     * @param hTable the value of hTable
     * @return the boolean
     */
    protected boolean checkIfRowExists(T row, Table hTable) throws IOException {
        Get get = new Get(Bytes.toBytes(row.getBBRowKey()));
        Result result = hTable.get(get);
        if (result.isEmpty()) {
            return false;
        } else {
            return true;
        }

    }
}
