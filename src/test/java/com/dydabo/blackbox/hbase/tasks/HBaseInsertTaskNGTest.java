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
package com.dydabo.blackbox.hbase.tasks;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.beans.Customer;
import com.dydabo.blackbox.beans.Employee;
import com.dydabo.blackbox.hbase.HBaseJsonImpl;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import org.apache.hadoop.hbase.client.Connection;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseInsertTaskNGTest {

    private final Connection connection;
    private Random random = new Random();

    public HBaseInsertTaskNGTest() throws IOException {
        this.connection = new HBaseJsonImpl<BlackBoxable>().getConnection();
        new HBaseUtils<BlackBoxable>().createTable(new Customer(111, "sss"), connection);
        new HBaseUtils<BlackBoxable>().createTable(new Employee(111, "sss"), connection);
    }

    @BeforeClass
    public static void setUpClass() throws Exception {

    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @BeforeMethod
    public void setUpMethod() throws Exception {
    }

    @AfterMethod
    public void tearDownMethod() throws Exception {
    }

    /**
     * Test of insert method, of class HBaseInsertTask.
     */
    @Test(dataProvider = "insertData")
    public void testInsert(BlackBoxable row, Boolean expResult) throws Exception {
        boolean checkExisting = true;
        HBaseInsertTask instance = new HBaseInsertTask(connection, row, checkExisting);
        Boolean result = instance.insert(row, checkExisting);
        Assert.assertEquals(result, expResult);
    }

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
        HBaseInsertTask instance = new HBaseInsertTask(connection, Collections.emptyList(), true);;
        Connection result = instance.getConnection();
        Assert.assertNotNull(result);
    }

}
