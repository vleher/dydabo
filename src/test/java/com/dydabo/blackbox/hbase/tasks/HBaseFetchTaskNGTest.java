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
import java.util.List;
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
public class HBaseFetchTaskNGTest {

    private final Connection connection;

    public HBaseFetchTaskNGTest() throws IOException {
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
     * Test of fetch method, of class HBaseFetchTask.
     */
    @Test
    public void testFetch(BlackBoxable row) throws Exception {
        HBaseFetchTask instance = new HBaseFetchTask(connection, row);
        int expResult = 0;
        List result = instance.fetch(row);
        Assert.assertEquals(result.size(), expResult);
    }

    /**
     * Test of compute method, of class HBaseFetchTask.
     */
    @Test
    public void testCompute(BlackBoxable row) {
        HBaseFetchTask instance = new HBaseFetchTask(connection, row);
        List result = instance.compute();
        int expResult = 0;
        Assert.assertEquals(result.size(), expResult);
    }

    /**
     * Test of getConnection method, of class HBaseFetchTask.
     */
    @Test
    public void testGetConnection() {
        HBaseFetchTask instance = new HBaseFetchTask(connection, null);
        Connection result = instance.getConnection();
        Assert.assertNotNull(result);
    }

}
