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
package com.dydabo.blackbox.hbase;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.test.beans.Employee;
import com.dydabo.blackbox.test.beans.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.testng.Assert;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
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
public class HBaseJsonImplNGTest {

    public HBaseJsonImplNGTest() {
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
     * Test of getConfig method, of class HBaseJsonImpl.
     */
    @Test
    public void testGetConnnection() {
        try {
            System.out.println("getConfig");
            HBaseJsonImpl instance = new HBaseJsonImpl();
            Connection result = instance.getConnection();
            Assert.assertNotNull(result);
        } catch (IOException ex) {
            Logger.getLogger(HBaseJsonImplNGTest.class.getName()).log(Level.SEVERE, null, ex);
            fail(ex.getMessage(), ex);
        }
    }

    @DataProvider(name = "testDeleteDataGT")
    public Object[][] testDeleteDataGT() {
        return new Object[][]{
            {new Employee(1, "name 1")}
        };
    }

    /**
     * Test of delete method, of class HBaseJsonImpl.
     */
    @Test(dataProvider = "testDeleteDataGT")
    public void testDelete_GenericType(BlackBoxable row) throws Exception {
        System.out.println("delete");
        HBaseJsonImpl instance = new HBaseJsonImpl();
        boolean expResult = true;
        boolean result = instance.delete(row);
        assertEquals(result, expResult);
    }

    @DataProvider(name = "insertgenericdata")
    public Object[][] insertGenericData() {
        return new Object[][]{
            {new Employee(1111, "name 1")},
            {new User(1234, null)}
        };
    }

    /**
     * Test of insert method, of class HBaseJsonImpl.
     */
    @Test(dataProvider = "insertgenericdata")
    public void testInsert_GenericType(BlackBoxable row) throws Exception {
        HBaseJsonImpl instance = new HBaseJsonImpl();
        try {
            System.out.println("insert");
            instance.delete(row);
            boolean expResult = true;
            boolean result = instance.insert(row);
            assertEquals(result, expResult);
        } finally {
            instance.delete(row);
        }
    }

    @DataProvider(name = "selectgenericdata")
    public Object[][] selectGenericData() {
        return new Object[][]{
            {new Employee(2222, "name 1")}, //{new User(33333, null)}
        };
    }

    /**
     * Test of fetch method, of class HBaseJsonImpl.
     */
    @Test(dataProvider = "selectgenericdata")
    public void testSelect_GenericType(BlackBoxable row) throws Exception {
        System.out.println("select");
        HBaseJsonImpl instance = new HBaseJsonImpl();
        instance.update(row);
        List result = instance.select(row);
        assertEquals(result.size(), 1);
        instance.delete(row);
        result = instance.select(row);
        assertEquals(result.size(), 0);
    }

    @DataProvider(name = "testdeletelistdata")
    public Object[][] createDeleteListData() {
        return new Object[][]{
            {new ArrayList() {
            }}
        };
    }

    /**
     * Test of delete method, of class HBaseJsonImpl.
     */
    @Test(dataProvider = "testdeletelistdata")
    public void testDelete_List(List<BlackBoxable> rows) throws Exception {
        System.out.println("delete");
        HBaseJsonImpl instance = new HBaseJsonImpl();
        boolean expResult = true;
        boolean result = instance.delete(rows);
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
     * Test of createTable method, of class HBaseJsonImpl.
     */
    @Test(dataProvider = "createtabledata")
    public void testCreateTable(BlackBoxable row) throws Exception {
        System.out.println("createTable");
        HBaseJsonImpl instance = new HBaseJsonImpl();
        Admin admin = instance.getConnection().getAdmin();
        boolean expResult = true;
        boolean result = instance.createTable(row, admin);
        assertEquals(result, expResult);
    }

    @DataProvider(name = "gettablenamedata")
    public Object[][] tableNameData() {
        return new Object[][]{
            {new Employee(1, null), "comdydaboblackboxtestbeansEmployee"}
        };
    }

    /**
     * Test of getTableName method, of class HBaseJsonImpl.
     */
    @Test(dataProvider = "gettablenamedata")
    public void testGetTableName(BlackBoxable row, String tableName) throws IOException {
        System.out.println("getTableName");
        HBaseJsonImpl instance = new HBaseJsonImpl();
        TableName expResult = TableName.valueOf(tableName);
        TableName result = instance.getTableName(row);
        assertEquals(result, expResult);
    }

    @DataProvider(name = "rowexistsdata")
    public Object[][] rowExistsData() {
        return new Object[][]{
            {new Employee(1010101345, null), false},
            {new Employee(12312312, null), true}
        };
    }

    /**
     * Test of checkIfRowExists method, of class HBaseJsonImpl.
     */
    @Test(dataProvider = "rowexistsdata")
    public void testCheckIfRowExists(BlackBoxable row, boolean expResult) throws Exception {
        System.out.println("checkIfRowExists");
        HBaseJsonImpl instance = new HBaseJsonImpl();
        Admin admin = instance.getConnection().getAdmin();
        try {
            Table hTable = admin.getConnection().getTable(instance.getTableName(row));
            if (hTable != null) {

                if (expResult) {
                    instance.update(row);
                }

                boolean result = instance.checkIfRowExists(row, hTable);
                assertEquals(result, expResult);

                instance.delete(row);
            }
        } catch (DoNotRetryIOException ex) {
            //ignore
        }
    }

}
