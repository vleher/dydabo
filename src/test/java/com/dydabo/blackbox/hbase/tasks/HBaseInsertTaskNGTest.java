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
import java.util.Random;

/**
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseInsertTaskNGTest {

    private Connection connection = null;
    private final Random random = new Random();

    /**
     * @throws IOException
     * @throws BlackBoxException
     */
    public HBaseInsertTaskNGTest() throws BlackBoxException {
        try {
            this.connection = new HBaseBlackBoxImpl<>().getConnection();
            if (connection != null) {
                new HBaseUtils<>().createTable(new Customer(111, "sss"), connection);
                new HBaseUtils<>().createTable(new Employee(111, "sss"), connection);
            }
        } catch (IOException ex) {
            //
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
     * Test of insert method, of class HBaseInsertTask.
     *
     * @param row
     * @param expResult
     * @throws java.lang.Exception
     */
    @Test(dataProvider = "insertData")
    public void testInsert(BlackBoxable row, Boolean expResult) throws Exception {
        if (connection == null) return;
        HBaseInsertTask instance = new HBaseInsertTask(connection, row, true);
        Boolean result = instance.insert(row, true);
        Assert.assertEquals(result, expResult);
    }

    /**
     * @return
     */
    @DataProvider(name = "insertData")
    public Object[][] insertData() {
        Customer custOne = new Customer(random.nextInt(10000), "Larry David");
        custOne.initData();
        Customer custTwo = new Customer(random.nextInt(10000), "ABCD");
        custTwo.initData();
        Customer custThree = new Customer(random.nextInt(10000), "Test One");
        custThree.initData();
        return new Object[][]{
                {custOne, true}
        };
    }

    /**
     * Test of getConnection method, of class HBaseInsertTask.
     */
    @Test
    public void testGetConnection() {
        if (connection == null) return;
        HBaseInsertTask instance = new HBaseInsertTask(connection, Collections.emptyList(), true);
        Connection result = instance.getConnection();
        Assert.assertNotNull(result);
    }

}
