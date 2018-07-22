/*
 * Copyright 2017 viswadas leher .
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.dydabo.blackbox.cassandra.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.IndexMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.utils.DBUtils;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.db.CassandraConnectionManager;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @param <T>
 * @author viswadas leher
 */
public class CassandraUtils<T extends BlackBoxable> implements DBUtils<T> {

    private final Logger logger = Logger.getLogger(CassandraUtils.class.getName());

    private String getDatabaseType(Type type) {
        String dbType = "text";
        if (type == String.class) {
            dbType = "varchar";
        } else if (type == Integer.class) {
            dbType = "varint";
        } else if (type == Float.class) {
            dbType = "float";
        } else if (type == Double.class || type == Long.class) {
            dbType = "double";
        } else if (type == Boolean.class) {
            dbType = "Boolean";
        } else if (type == List.class || type == Set.class) {
            dbType = "text";
        } else if (type == Map.class) {
            dbType = "text";
        }

        return dbType;
    }

    /**
     * @param row
     * @return
     */
    public String getTableName(T row) {
        return row.getClass().toString().substring(6).replaceAll("\\.", "");
    }

    /**
     * @param row
     * @return
     */
    public boolean createTable(T row) {

        try (Cluster cluster = CassandraConnectionManager.getCluster()) {
            // create table
            TableMetadata table = cluster.getMetadata().getKeyspace(CassandraConstants.KEYSPACE)
                    .getTable(getTableName(row));
            if (table == null) {
                StringBuilder query = new StringBuilder("create table " + getTableName(row) + "(" + CassandraConstants.DEFAULT_ROWKEY + " text primary key");

                Map<String, Field> fields = DyDaBoUtils.getFieldFromType(row.getClass());

                for (Map.Entry<String, Field> entry : fields.entrySet()) {
                    String fName = entry.getKey();
                    Field field = entry.getValue();
                    if (!field.isSynthetic()) {
                        String dbType = getDatabaseType(field.getType());
                        query.append(", \"").append(fName).append("\" ").append(dbType);
                    }
                }

                query.append(");");
                try (Session session = CassandraConnectionManager.getSession()) {
                    logger.finer("Create Table:" + query);
                    session.execute(query.toString());
                }
            }
        }
        return true;
    }

    /**
     * @param columnName
     * @param row
     * @return
     */
    public boolean createIndex(String columnName, T row) {
        String columnIndexName = columnName + "idx";

        try (Cluster cluster = CassandraConnectionManager.getCluster()) {
            TableMetadata table = cluster.getMetadata().getKeyspace(CassandraConstants.KEYSPACE)
                    .getTable(getTableName(row));

            for (IndexMetadata indexMetadata : table.getIndexes()) {
                if (columnIndexName.equalsIgnoreCase(indexMetadata.getName())) {
                    return true;
                }
            }

            String indexQuery = "create custom index if not exists \"" + columnIndexName + "\" " +
                    "on " + CassandraConstants.KEYSPACE + "." + getTableName(row) + "(\"" + columnName + "\") USING 'org.apache.cassandra.index.sasi.SASIIndex' " +
                    "with OPTIONS = {'mode':'CONTAINS'};";
            logger.finer("Query:" + indexQuery);
            // create the index
            try (Session session = CassandraConnectionManager.getSession()) {
                session.execute(indexQuery);
            }

            int count = 0;
            // TODO: clean this up
            while (table.getIndex(columnIndexName) == null && count < 2) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
                count++;
            }
        }
        return true;
    }

}
