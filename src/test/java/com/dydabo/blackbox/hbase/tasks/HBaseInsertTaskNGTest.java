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
import com.dydabo.blackbox.hbase.HBaseJsonImpl;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Connection;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseInsertTaskNGTest {

    private final Connection connection;

    public HBaseInsertTaskNGTest() throws IOException {
        this.connection = new HBaseJsonImpl<BlackBoxable>().getConnection();
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
    @Test
    public void testInsert(BlackBoxable row, Boolean expResult) throws Exception {
        boolean checkExisting = true;
        HBaseInsertTask instance = new HBaseInsertTask(connection, row, checkExisting);
        Boolean result = instance.insert(row, checkExisting);
        Assert.assertEquals(result, expResult);

    }

    /**
     * Test of compute method, of class HBaseInsertTask.
     */
    @Test
    public void testCompute(BlackBoxable row, Boolean expResult) {
        boolean checkExisting = true;
        HBaseInsertTask instance = new HBaseInsertTask(connection, row, checkExisting);
        Boolean result = instance.compute();
        Assert.assertEquals(result, expResult);
    }

    /**
     * Test of getConnection method, of class HBaseInsertTask.
     */
    @Test
    public void testGetConnection() {
        HBaseInsertTask instance = new HBaseInsertTask(connection, null, true);;
        Connection result = instance.getConnection();
        Assert.assertNotNull(result);
    }

}
