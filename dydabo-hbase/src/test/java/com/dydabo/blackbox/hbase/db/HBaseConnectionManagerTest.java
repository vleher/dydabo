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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.io.IOException;

/**
 * @author viswadas leher
 */
public class HBaseConnectionManagerTest {

    @Mock
    private static Connection connection;
    private static HBaseConnectionManager connectionManager;

    @BeforeAll
    static void setUp() throws IOException {
        connectionManager = new HBaseConnectionManager(HBaseConfiguration.create());
    }

    /**
     * Test of getConnection method, of class HBaseConnectionManager.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testGetConnection() throws Exception {
//        Connection result = connectionManager.getConnection();
//        Assertions.assertNotNull(result);
//
//        // close the connection and test
//        result.close();
//        Connection resultOne = connectionManager.getConnection();
//        Assertions.assertNotNull(resultOne);
//        Assertions.assertNotEquals(result, resultOne);
    }

    /**
     * Test of getConnection method, of class HBaseConnectionManager.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testGetConnection_Configuration() throws Exception {
//        Configuration config = HBaseConfiguration.create();
//        Connection result = connectionManager.getConnection(config);
//        Assertions.assertNotNull(result);
    }
}
