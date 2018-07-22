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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author viswadas leher
 */
public class HBaseConnectionManagerTest {



    /**
     * Test of getConnection method, of class HBaseConnectionManager.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testGetConnection() throws Exception {

        Connection result = HBaseConnectionManager.getConnection();
        assertNotNull(result);

        // close the connection and test
        result.close();
        Connection resultOne = HBaseConnectionManager.getConnection();
        assertNotNull(resultOne);
        assertNotEquals(result, resultOne);
    }

    /**
     * Test of getConnection method, of class HBaseConnectionManager.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testGetConnection_Configuration() throws Exception {
        Configuration config = HBaseConfiguration.create();
        Connection result = HBaseConnectionManager.getConnection(config);
        assertNotNull(result);
    }

    /**
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    @Test
    public void testConstructorIsPrivate() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<HBaseConnectionManager> constructor = HBaseConnectionManager.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        constructor.newInstance();
    }

}
