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
package com.dydabo.blackbox.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.dydabo.blackbox.cassandra.utils.CassandraConstants;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author viswadas leher
 */
public class CassandraConnectionManager {

    private static final Logger logger = Logger.getLogger(CassandraConnectionManager.class.getName());
    private static final Map<String, Session> sessionPool = new HashMap<>();
    private static final Map<String, Cluster> clusterPool = new HashMap<>();

    private static String address;

    private CassandraConnectionManager() {
    }

    // TODO: clean this up

    /**
     * @param CassandraConstants.KEYSPACE
     * @param address
     * @return
     */
    public static synchronized Session getSession(String address) {

        if (sessionPool.get(CassandraConstants.KEYSPACE) == null) {
            if (clusterPool.get(CassandraConstants.CLUSTER_NAME) == null) {
                Cluster cluster = Cluster.builder()
                        .withClusterName(CassandraConstants.CLUSTER_NAME)
                        .addContactPoint(address)
                        .build();
                clusterPool.put(CassandraConstants.CLUSTER_NAME, cluster);
            }

            Cluster cluster = clusterPool.get(CassandraConstants.CLUSTER_NAME);
            Session session = cluster.connect();

            // keysapce query TODO: create a CassandraConstants.KEYSPACE name, make it all configurable
            String ksQuery = "create keyspace if not exists " + CassandraConstants.KEYSPACE + " with replication = {'class':'SimpleStrategy', 'replication_factor':1};";
            session.execute(ksQuery);
            session = cluster.connect(CassandraConstants.KEYSPACE);
            sessionPool.put(CassandraConstants.KEYSPACE, session);
        }

        return sessionPool.get(CassandraConstants.KEYSPACE);
    }

    public static Session getSession() {
        return getSession(address);
    }

    /**
     * @param clusterName
     * @param CassandraConstants.KEYSPACE
     * @param address
     * @return
     */
    public static synchronized Cluster getCluster() {
        if (clusterPool.get(CassandraConstants.CLUSTER_NAME) == null) {
            Cluster cluster = Cluster.builder()
                    .withClusterName(CassandraConstants.CLUSTER_NAME)
                    .addContactPoint(address)
                    .build();
            clusterPool.put(CassandraConstants.CLUSTER_NAME, cluster);
        }
        Session session = clusterPool.get(CassandraConstants.CLUSTER_NAME).connect();

        // keysapce query TODO: create a CassandraConstants.KEYSPACE name
        String ksQuery = "create keyspace if not exists " + CassandraConstants.KEYSPACE + " with replication = {'class':'SimpleStrategy', 'replication_factor':1};";
        session.execute(ksQuery);
        return clusterPool.get(CassandraConstants.CLUSTER_NAME);
    }

    public static String getAddress() {
        return address;
    }

    public static void setAddress(String address) {
        CassandraConnectionManager.address = address;
    }

}
