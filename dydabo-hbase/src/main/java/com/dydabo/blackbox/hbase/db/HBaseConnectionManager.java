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
package com.dydabo.blackbox.hbase.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author viswadas leher
 */
public class HBaseConnectionManager {

    private static final Object lockObject = new Object();

    private static final Configuration defaultConfig = HBaseConfiguration.create();
    private static final Map<Integer, Connection> connectionPool = new HashMap<>();
    private static final Logger logger = Logger.getLogger(HBaseConnectionManager.class.getName());

    private HBaseConnectionManager() {
    }

    /**
     * @return
     * @throws IOException
     */
    public static Connection getConnection() throws IOException {
        return getConnection(defaultConfig);
    }

    /**
     * @param config
     * @return
     * @throws IOException
     */
    public static Connection getConnection(Configuration config) throws IOException {
        Connection thisConnection = connectionPool.get(config.hashCode());
        if (thisConnection == null || thisConnection.isAborted() || thisConnection.isClosed()) {
            synchronized (lockObject) {
                thisConnection = ConnectionFactory.createConnection(config);
            }
        }
        if (thisConnection != null) {
            connectionPool.put(config.hashCode(), thisConnection);
        }
        return thisConnection;
    }

    /**
     * Close all open connections. This can be called by the client to do a graceful shutdown.
     */
    public static synchronized void closeAllConnections() {

        connectionPool.forEach((integer, connection) -> {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        });

    }
}
