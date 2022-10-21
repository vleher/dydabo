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
package com.dydabo.blackbox.cassandra.db;

import com.datastax.oss.driver.api.core.CqlSession;
import com.dydabo.blackbox.cassandra.utils.CassandraConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;

/**
 * @author viswadas leher
 */
public class CassandraConnectionManager {

    private final Logger logger = LogManager.getLogger();
    private final String address;
    private final int port;

    public CassandraConnectionManager(String address, int port) {
        this.address = address;
        this.port = port;
        String ksQuery = "create keyspace if not exists " + CassandraConstants.KEYSPACE + " with replication = " + "{'class" +
                "':'SimpleStrategy', 'replication_factor':1};";
        getSession().execute(ksQuery);
    }

    public CqlSession getSession() {
        return CqlSession.builder().addContactPoint(new InetSocketAddress(address, port)).withKeyspace(CassandraConstants.KEYSPACE).build();
    }
}
