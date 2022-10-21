/*
 *  Copyright 2020 viswadas leher <vleher@gmail.com>.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.dydabo.blackbox.cassandra.utils;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import com.dydabo.blackbox.common.utils.DyDaBoDBUtils;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @param <T>
 * @author viswadas leher
 */
public class CassandraUtils<T extends BlackBoxable> implements DyDaBoDBUtils<T> {

    private final Logger logger = LogManager.getLogger();
    private final CassandraConnectionManager connectionManager;

    public CassandraUtils(CassandraConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

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
        return row.getClass().getSimpleName();
    }

    /**
     * @param row
     * @return
     */
    public boolean createTable(T row) {
        // create table
        Optional<KeyspaceMetadata> keyspaceMetadata =
                connectionManager.getSession().getMetadata().getKeyspace(CassandraConstants.KEYSPACE);

        if (keyspaceMetadata.isPresent()) {
            Optional<TableMetadata> table = keyspaceMetadata.get().getTable(getTableName(row));
            if (table.isPresent()) {
                return true;
            } else {
                StringBuilder query =
                        new StringBuilder("create table " + getTableName(row) + "(" + CassandraConstants.DEFAULT_ROWKEY + " text "
                                + "primary key");

                Map<String, Field> fields = DyDaBoUtils.getFieldFromType(row.getClass());

                for (Map.Entry<String, Field> entry : fields.entrySet()) {
                    String fName = entry.getKey();
                    Field field = entry.getValue();
                    if (!field.isSynthetic()) {
                        String dbType = getDatabaseType(field.getType());
                        query.append(",").append(fName).append(" ").append(dbType);
                    }
                }

                query.append(");");
                logger.info("Create Table: {}", query);
                ResultSet resultSet = connectionManager.getSession().execute(SimpleStatement.builder(query.toString()).build());
                return true;
            }
        }

        return false;
    }

    /**
     * @param columnName
     * @param row
     * @return
     */
    public boolean createIndex(String columnName, T row) {
        String columnIndexName = columnName + "idx";
        Optional<KeyspaceMetadata> keyspace = connectionManager.getSession().getMetadata().getKeyspace(CassandraConstants.KEYSPACE);

        Optional<TableMetadata> table = keyspace.flatMap(keyspaceMetadata -> keyspaceMetadata.getTable(getTableName(row)));

        if (table.isPresent() && (table.get().getIndex(columnIndexName).isPresent())) {
            return true;
        }

        String indexQuery =
                "create custom index if not exists \"" + columnIndexName + "\" " + "on " + CassandraConstants.KEYSPACE + "." + getTableName(row) + "(\"" + columnName + "\") USING 'org.apache.cassandra.index.sasi.SASIIndex' " + "with OPTIONS = {'mode':'CONTAINS'};";
        logger.debug("Index Query: {}", indexQuery);
        // create the index
        connectionManager.getSession().execute(indexQuery);
        return true;
    }
}
