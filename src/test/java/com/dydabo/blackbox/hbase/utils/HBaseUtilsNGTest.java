/** *****************************************************************************
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************
 */
package com.dydabo.blackbox.hbase.utils;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.beans.Customer;
import com.dydabo.blackbox.beans.Employee;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.blackbox.hbase.HBaseBlackBoxImpl;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.Collections;

import static org.testng.Assert.assertEquals;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseUtilsNGTest {

    /**
     *
     */
    public HBaseUtilsNGTest() {
    }

    /**
     *
     * @throws Exception
     */
    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    /**
     *
     * @throws Exception
     */
    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    /**
     *
     * @throws Exception
     */
    @BeforeMethod
    public void setUpMethod() throws Exception {
    }

    /**
     *
     * @throws Exception
     */
    @AfterMethod
    public void tearDownMethod() throws Exception {
    }

    /**
     * Test of getTableName method, of class HBaseBlackBoxImpl.
     *
     * @param row
     * @param tableName
     *
     * @throws java.io.IOException
     */
    @Test(dataProvider = "gettablenamedata")
    public void testGetTableName(BlackBoxable row, String tableName) throws IOException {
        TableName expResult = TableName.valueOf(tableName);
        TableName result = new HBaseUtils().getTableName(row);
        assertEquals(result, expResult);
    }

    /**
     *
     * @return
     */
    @DataProvider(name = "gettablenamedata")
    public Object[][] tableNameData() {
        return new Object[][]{
            {new Employee(1, null), "comdydaboblackboxbeansEmployee"}
        };
    }

    /**
     * Test of createTable method, of class HBaseBlackBoxImpl.
     *
     * @param row
     *
     * @throws java.lang.Exception
     */
    @Test(dataProvider = "createtabledata")
    public void testCreateTable(BlackBoxable row) throws Exception {
        HBaseBlackBoxImpl instance = new HBaseBlackBoxImpl();
        boolean expResult = true;
        boolean result = new HBaseUtils().createTable(row, instance.getConnection());
        assertEquals(result, expResult);
    }

    /**
     *
     * @return
     */
    @DataProvider(name = "createtabledata")
    public Object[][] createTableData() {
        return new Object[][]{
            {new Customer(null, null)},
            {new Employee(null, null)}
        };
    }

    /**
     * Test of convertRowToHTable method, of class HBaseUtils.
     *
     * @throws com.dydabo.blackbox.BlackBoxException
     */
    @Test
    public void testConvertJsonToMap() throws JsonSyntaxException, BlackBoxException {
        HBaseUtils instance = new HBaseUtils();
        GenericDBTableRow result = instance.convertRowToTableRow(new Customer(12, "1234"));
        Assert.assertNotNull(result);
    }

    /**
     * Test of checkIfRowExists method, of class HBaseUtils.
     *
     * @param row
     * @param expResult
     *
     * @throws java.lang.Exception
     */
    @Test(dataProvider = "rowexistsdata")
    public void testCheckIfRowExists(BlackBoxable row, boolean expResult) throws Exception {
        HBaseUtils instance = new HBaseUtils();
        final HBaseBlackBoxImpl<BlackBoxable> hBaseJsonImpl = new HBaseBlackBoxImpl<>();
        Admin admin = hBaseJsonImpl.getConnection().getAdmin();
        try {
            Table hTable = admin.getConnection().getTable(instance.getTableName(row));
            if (hTable != null) {
                if (expResult) {
                    hBaseJsonImpl.update(Collections.singletonList(row));
                }

                boolean result = instance.checkIfRowExists(row, hTable);
                assertEquals(result, expResult);

                hBaseJsonImpl.delete(Collections.singletonList(row));
            }
        } catch (DoNotRetryIOException ex) {
            //ignore
        }
    }

    /**
     *
     * @return
     */
    @DataProvider(name = "rowexistsdata")
    public Object[][] rowExistsData() {
        return new Object[][]{
            {new Employee(1010101345, "Dummy Name"), false},
            {new Employee(12312312, "Adele"), true}
        };
    }

}
