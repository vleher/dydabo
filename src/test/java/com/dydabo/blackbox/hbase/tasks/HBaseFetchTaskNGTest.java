/*
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
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
package com.dydabo.blackbox.hbase.tasks;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.beans.Customer;
import com.dydabo.blackbox.beans.Employee;
import com.dydabo.blackbox.hbase.HBaseBlackBoxImpl;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseFetchTaskNGTest {

    private Connection connection;

    /**
     * @throws IOException
     * @throws BlackBoxException
     */
    public HBaseFetchTaskNGTest() throws BlackBoxException {
        try {
            this.connection = new HBaseBlackBoxImpl<>().getConnection();

        if (connection != null) {
            new HBaseUtils<>().createTable(new Customer(111, "sss"), connection);
            new HBaseUtils<>().createTable(new Employee(111, "sss"), connection);
        }} catch (IOException e) {
            connection = null;
        }
    }

    /**
     * @throws Exception
     */
    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    /**
     * @throws Exception
     */
    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    /**
     * @throws Exception
     */
    @BeforeMethod
    public void setUpMethod() throws Exception {
    }

    /**
     * @throws Exception
     */
    @AfterMethod
    public void tearDownMethod() throws Exception {
    }

    /**
     * Test of fetch method, of class HBaseFetchTask.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testFetch() throws Exception {
        List<String> rowKeys = new ArrayList<>();
        rowKeys.add("1");
        rowKeys.add("2");
        final Customer cust = new Customer(null, null);

        HBaseFetchTask instance = new HBaseFetchTask(connection, rowKeys, cust, false);
        List result = instance.fetch(rowKeys);
        Assert.assertNotNull(result);

    }

    /**
     * Test of compute method, of class HBaseFetchTask.
     */
    @Test
    public void testCompute() {

    }

    /**
     * Test of getConnection method, of class HBaseFetchTask.
     */
    @Test
    public void testGetConnection() {
        HBaseFetchTask instance = new HBaseFetchTask(connection, Collections.emptyList(), null, false);
        Connection result = instance.getConnection();
        Assert.assertNotNull(result);
    }

}
