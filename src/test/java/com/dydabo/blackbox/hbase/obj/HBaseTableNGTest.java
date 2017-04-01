/*******************************************************************************
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
 *******************************************************************************/
package com.dydabo.blackbox.hbase.obj;

import java.util.Map;
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
public class HBaseTableNGTest {

    /**
     *
     */
    protected static final String KEY = "key";
    private static HBaseTableRow instance;

    /**
     *
     */
    public HBaseTableNGTest() {
        this.instance = new HBaseTableRow(KEY);
    }

    /**
     *
     * @throws Exception
     */
    @BeforeClass
    public static void setUpClass() throws Exception {
        instance.createFamily("default");
        HBaseTableRow.ColumnFamily fam = instance.getColumnFamilies().get("default");
        fam.addColumn("col1", "colVal1");
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
     * Test of getDefaultFamily method, of class HBaseTableRow.
     */
    @Test
    public void testGetDefaultFamily() {
        HBaseTableRow.ColumnFamily result = instance.getDefaultFamily();
        Assert.assertNotNull(result);
    }

    /**
     * Test of createFamily method, of class HBaseTableRow.
     */
    @Test
    public void testCreateFamily() {
        String familyName = "testFamily";
        HBaseTableRow.ColumnFamily result = instance.createFamily(familyName);
        Assert.assertNotNull(result);
    }

    /**
     * Test of getRowKey method, of class HBaseTableRow.
     */
    @Test
    public void testGetRowKey() {
        String expResult = KEY;
        String result = instance.getRowKey();
        Assert.assertEquals(result, expResult);
    }

    /**
     * Test of setRowKey method, of class HBaseTableRow.
     */
    @Test
    public void testSetRowKey() {
        String rowKey = "newKey";
        instance.setRowKey(rowKey);

        Assert.assertEquals(instance.getRowKey(), rowKey);
        //reset the key
        instance.setRowKey(KEY);
    }

    /**
     * Test of getColumnFamilies method, of class HBaseTableRow.
     */
    @Test
    public void testGetColumnFamilies() {
        Map result = instance.getColumnFamilies();
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    /**
     * Test of toString method, of class HBaseTableRow.
     */
    @Test
    public void testToString() {
        String result = instance.toString();
        Assert.assertNotNull(result);
    }

}
