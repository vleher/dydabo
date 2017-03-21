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
package com.dydabo.blackbox.hbase.utils;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.beans.Employee;
import com.dydabo.blackbox.beans.User;
import com.dydabo.blackbox.hbase.HBaseJsonImpl;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseUtilsNGTest {

    public HBaseUtilsNGTest() {
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
     * Test of generateJson method, of class HBaseUtils.
     */
    @Test
    public void testGenerateJson() {
        HashMap<String, String> valueTable = new HashMap<>();
        HBaseUtils instance = new HBaseUtils();
        String result = instance.generateJson(valueTable);
        Assert.assertNotNull(result);
    }

    /**
     * Test of getTableName method, of class HBaseJsonImpl.
     */
    @Test(dataProvider = "gettablenamedata")
    public void testGetTableName(BlackBoxable row, String tableName) throws IOException {
        TableName expResult = TableName.valueOf(tableName);
        TableName result = new HBaseUtils().getTableName(row);
        assertEquals(result, expResult);
    }

    @DataProvider(name = "gettablenamedata")
    public Object[][] tableNameData() {
        return new Object[][]{
            {new Employee(1, null), "comdydaboblackboxbeansEmployee"}
        };
    }

    /**
     * Test of createTable method, of class HBaseJsonImpl.
     */
    @Test(dataProvider = "createtabledata")
    public void testCreateTable(BlackBoxable row) throws Exception {
        HBaseJsonImpl instance = new HBaseJsonImpl();
        Admin admin = instance.getConnection().getAdmin();
        boolean expResult = true;
        boolean result = new HBaseUtils().createTable(row, admin);
        assertEquals(result, expResult);
    }

    @DataProvider(name = "createtabledata")
    public Object[][] createTableData() {
        return new Object[][]{
            {new User(null, null)},
            {new Employee(null, null)}
        };
    }

    /**
     * Test of convertJsonToMap method, of class HBaseUtils.
     */
    @Test
    public void testConvertJsonToMap(BlackBoxable row, Map<String, String> valueMap) {
        HBaseUtils instance = new HBaseUtils();
        Map expResult = null;
        Map result = instance.convertJsonToMap(row, valueMap);
        assertEquals(result, expResult);
    }

    /**
     * Test of checkIfRowExists method, of class HBaseUtils.
     */
    @Test(dataProvider = "rowexistsdata")
    public void testCheckIfRowExists(BlackBoxable row, boolean expResult) throws Exception {
        HBaseUtils instance = new HBaseUtils();
        final HBaseJsonImpl<BlackBoxable> hBaseJsonImpl = new HBaseJsonImpl<>();
        Admin admin = hBaseJsonImpl.getConnection().getAdmin();
        try {
            Table hTable = admin.getConnection().getTable(new HBaseUtils().getTableName(row));
            if (hTable != null) {

                if (expResult) {
                    hBaseJsonImpl.update(Arrays.asList(row));
                }

                boolean result = instance.checkIfRowExists(row, hTable);
                assertEquals(result, expResult);

                hBaseJsonImpl.delete(Arrays.asList(row));
            }
        } catch (DoNotRetryIOException ex) {
            //ignore
        }
    }

    @DataProvider(name = "rowexistsdata")
    public Object[][] rowExistsData() {
        return new Object[][]{
            {new Employee(1010101345, null), false},
            {new Employee(12312312, null), true}
        };
    }

}
