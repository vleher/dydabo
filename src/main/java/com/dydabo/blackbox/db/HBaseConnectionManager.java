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
package com.dydabo.blackbox.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseConnectionManager {

    private static Object lockObject = new Object();

    private static Configuration defaultConfig = HBaseConfiguration.create();
    private static Map<Integer, Connection> connectionPool = new HashMap<>();

    private HBaseConnectionManager() {
    }

    /**
     *
     * @return
     * @throws IOException
     */
    public static Connection getConnection() throws IOException {
        return getConnection(defaultConfig);
    }

    /**
     *
     * @param config
     * @return
     * @throws IOException
     */
    public static Connection getConnection(Configuration config) throws IOException {
        Connection thisConnection = connectionPool.get(config.hashCode());
        if (thisConnection == null) {
            synchronized (lockObject) {
                thisConnection = ConnectionFactory.createConnection(config);
            }
        } else if (thisConnection.isAborted() || thisConnection.isClosed()) {
            synchronized (lockObject) {
                thisConnection = ConnectionFactory.createConnection(config);
            }
        }
        connectionPool.put(config.hashCode(), thisConnection);
        return thisConnection;
    }

}
