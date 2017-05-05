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
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.beans.Customer;
import com.dydabo.blackbox.beans.Employee;
import com.dydabo.blackbox.hbase.HBaseBlackBoxImpl;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseSearchTaskNGTest {

    private Connection connection;

    /**
     * @throws IOException
     * @throws BlackBoxException
     */
    public HBaseSearchTaskNGTest() throws IOException, BlackBoxException {
        try {
            this.connection = new HBaseBlackBoxImpl<>().getConnection();
            new HBaseUtils<>().createTable(new Customer(111, "sss"), connection);
            new HBaseUtils<>().createTable(new Employee(111, "sss"), connection);
        } catch (IOException exp) {
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
     * Test of search method, of class HBaseSearchTask.
     *
     * @param row
     * @throws java.lang.Exception
     */
    @Test(dataProvider = "searchData")
    public void testSearch(BlackBoxable row) throws Exception {
        HBaseSearchTask instance = new HBaseSearchTask(connection, row, -1);
        List result = instance.search(row);
        Assert.assertNotNull(result);
    }

    /**
     * @return
     */
    @DataProvider(name = "searchData")
    public Object[][] searchData() {
        return new Object[][]{
                {new Customer(2, null)}
        };
    }

    /**
     * Test of getConnection method, of class HBaseSearchTask.
     */
    @Test
    public void testGetConnection() {
        HBaseSearchTask instance = new HBaseSearchTask(connection, Collections.emptyList(), -1);
        Connection result = instance.getConnection();
        Assert.assertNotNull(result);
    }

}
