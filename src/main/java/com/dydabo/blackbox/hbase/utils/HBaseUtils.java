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
package com.dydabo.blackbox.hbase.utils;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.hbase.HBaseJsonImpl;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseUtils<T extends BlackBoxable> {

    private SortedSet tableCache = new TreeSet();

    public String generateJson(HashMap<String, String> valueTable) {
        String jsonString = new Gson().toJson(valueTable);
        return jsonString;
    }

    public TableName getTableName(T row) {
        final String fullClassName = row.getClass().toString().substring(6).replaceAll("\\.", "");
        return TableName.valueOf(fullClassName);
    }

    /**
     *
     * @param row   the value of row
     * @param admin the value of admin
     *
     * @return the boolean
     *
     * @throws java.io.IOException
     */
    public boolean createTable(T row, Admin admin) throws IOException {
        TableName tableName = getTableName(row);
        if (!tableCache.contains(tableName) && !admin.tableExists(tableName)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor dFamily = new HColumnDescriptor(HBaseJsonImpl.DEFAULT_FAMILY);
            tableDescriptor.addFamily(dFamily);
            admin.createTable(tableDescriptor);
            tableCache.add(tableName);
        }
        return true;
    }

    public Map<String, String> convertJsonToMap(T row, Map<String, String> valueMap) throws JsonSyntaxException {
        JsonParser jsonParser = new JsonParser();
        JsonElement jsonTree = jsonParser.parse(row.getBBJson());
        for (Map.Entry<String, JsonElement> entry : jsonTree.getAsJsonObject().entrySet()) {
            JsonElement val = entry.getValue();
            System.out.println(entry.getKey() + ":" + val.toString() + " :" + val.getClass());
            System.out.println(val.getAsString());
            valueMap.put(entry.getKey(), val.getAsString());
        }
        return valueMap;
    }

    /**
     *
     * @param row    the value of row
     * @param hTable the value of hTable
     *
     * @return the boolean
     */
    public boolean checkIfRowExists(T row, Table hTable) throws IOException {
        Get get = new Get(Bytes.toBytes(row.getBBRowKey()));
        Result result = hTable.get(get);
        if (result.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }

}
