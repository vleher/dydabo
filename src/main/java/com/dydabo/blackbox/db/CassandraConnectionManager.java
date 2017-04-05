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
package com.dydabo.blackbox.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class CassandraConnectionManager {

    private static final Logger logger = Logger.getLogger(CassandraConnectionManager.class.getName());
    private static Map<String, Session> sessionPool = new HashMap<>();
    private static Map<String, Cluster> clusterPool = new HashMap<>();

    private CassandraConnectionManager() {
    }

    // TODO: clean this up
    public static synchronized Session getSession(String keyspace) {

        if (sessionPool.get(keyspace) == null) {
            if (clusterPool.get("myCluster") == null) { // TODO: parametirize this
                Cluster cluster = Cluster.builder()
                        .withClusterName("myCluster")
                        .addContactPoint("127.0.0.1")
                        .build();
                clusterPool.put("myCluster", cluster);
            }

            Cluster cluster = clusterPool.get("myCluster");
            Session session = cluster.connect();

            // keysapce query TODO: create a keyspace name, make it all configurable
            String ksQuery = "create keyspace if not exists " + keyspace + " with replication = {'class':'SimpleStrategy', 'replication_factor':1};";
            session.execute(ksQuery);
            session = cluster.connect(keyspace);
            sessionPool.put(keyspace, session);
        }

        return sessionPool.get(keyspace);
    }

    public static synchronized Cluster getCluster(String clusterName, String keyspace) {
        if (clusterPool.get("myCluster") == null) {
            Cluster cluster = Cluster.builder()
                    .withClusterName("myCluster")
                    .addContactPoint("127.0.0.1")
                    .build();
            clusterPool.put("myCluster", cluster);
        }
        Session session = clusterPool.get("myCluster").connect();

        // keysapce query TODO: create a keyspace name
        String ksQuery = "create keyspace if not exists " + keyspace + " with replication = {'class':'SimpleStrategy', 'replication_factor':1};";
        session.execute(ksQuery);
        return clusterPool.get("myCluster");
    }
}
