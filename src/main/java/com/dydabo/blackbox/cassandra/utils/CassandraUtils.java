/*
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dydabo.blackbox.cassandra.utils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.DBUtils;
import com.dydabo.blackbox.common.DyDaBoUtils;
import com.dydabo.blackbox.db.CassandraConnectionManager;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class CassandraUtils<T extends BlackBoxable> extends DBUtils<T> {

    private final Logger logger = Logger.getLogger(CassandraUtils.class.getName());

    private String getDatabaseType(Type type) {
        logger.info(" type:" + type);
        if (type == String.class) {
            return "varchar";
        } else if (type == Integer.class) {
            return "varint";
        } else if (type == Float.class) {
            return "float";
        } else if (type == Double.class || type == Long.class) {
            return "double";
        } else if (type == Boolean.class) {
            return "Boolean";
        } else if (type == List.class || type == Set.class) {
            return "text";
        } else if (type == Map.class) {
            return "text";
        }

        return "text";
    }

    public String getTableName(T row) {
        final String fullClassName = row.getClass().toString().substring(6).replaceAll("\\.", "");
        return fullClassName;
    }

    public boolean createTable(T row) {
        // create table
        TableMetadata table = CassandraConnectionManager.getCluster("myCluster", "bb").getMetadata().getKeyspace("bb").getTable(getTableName(row));
        if (table == null) {

            String query = "create table " + getTableName(row) + "(" +
                    "bbkey text primary key";

            Map<String, Field> fields = DyDaBoUtils.getFieldFromType(row.getClass());

            for (Map.Entry<String, Field> entry : fields.entrySet()) {
                String fName = entry.getKey();
                Field field = entry.getValue();
                if (!field.isSynthetic()) {
                    String dbType = getDatabaseType(field.getType());
                    logger.info("Table::" + fName + " :" + field.getGenericType() + " :" + dbType);
                    query += ", \"" + fName + "\" " + dbType;
                }
            }

//            GenericDBTableRow cTable = convertRowToTableRow(row);
//            for (Map.Entry<String, GenericDBTableRow.ColumnFamily> entry : cTable.getColumnFamilies().entrySet()) {
//                String familyName = entry.getKey();
//                GenericDBTableRow.ColumnFamily family = entry.getValue();
//                for (Map.Entry<String, GenericDBTableRow.Column> columns : family.getColumns().entrySet()) {
//                    String colName = columns.getKey();
//                    GenericDBTableRow.Column colValue = columns.getValue();
//
//                    String dbType = getDatabaseType(colValue.getColumnValue());
//                    query += ", \"" + colName + "\" " + dbType;
//                }
//            }
            query += ");";
            Session session = CassandraConnectionManager.getSession("bb");
            logger.info("Create Table:" + query);
            session.execute(query);
        }
        return true;
    }

    public boolean createIndex(String columnName, T row) {
        String indexQuery = "create index if not exists on bb." + getTableName(row) + "(" + columnName + ");";
        ResultSet resultSet = CassandraConnectionManager.getSession("bb").execute(indexQuery);
        TableMetadata table = CassandraConnectionManager.getCluster("myCluster", "bb").getMetadata().getKeyspace("bb").getTable(getTableName(row));
        int count = 0;
        // TODO: clean this up
        while (table.getIndex(getTableName(row) + "_" + columnName + "_idx") == null && count < 2) {
            try {
                new Thread().sleep(1000);
            } catch (InterruptedException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
            count++;
        }

        return true;
    }

}
