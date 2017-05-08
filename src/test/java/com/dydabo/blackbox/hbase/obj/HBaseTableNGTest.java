/*
 * Copyright 2017 viswadas leher .
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
package com.dydabo.blackbox.hbase.obj;

import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.blackbox.db.obj.GenericDBTableRow.ColumnFamily;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.Map;

/**
 * @author viswadas leher
 */
public class HBaseTableNGTest {

    /**
     *
     */
    protected static final String KEY = "key";
    private static GenericDBTableRow instance;

    /**
     *
     */
    public HBaseTableNGTest() {
        instance = new GenericDBTableRow(KEY);
    }

    /**
     * @throws Exception
     */
    @BeforeClass
    public static void setUpClass() throws Exception {
        instance.createFamily("default");
        GenericDBTableRow.ColumnFamily fam = instance.getColumnFamilies().get("default");
        fam.addColumn("col1", "colVal1");
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
     * Test of getDefaultFamily method, of class GenericDBTableRow.
     */
    @Test
    public void testGetDefaultFamily() {
        GenericDBTableRow.ColumnFamily result = instance.getDefaultFamily();
        Assert.assertNotNull(result);
    }

    /**
     * Test of createFamily method, of class GenericDBTableRow.
     */
    @Test
    public void testCreateFamily() {
        String familyName = "testFamily";
        GenericDBTableRow.ColumnFamily result = instance.createFamily(familyName);
        Assert.assertNotNull(result);
    }

    /**
     * Test of getRowKey method, of class GenericDBTableRow.
     */
    @Test
    public void testGetRowKey() {
        String result = instance.getRowKey();
        Assert.assertEquals(result, KEY);
    }

    /**
     * Test of setRowKey method, of class GenericDBTableRow.
     */
    @Test
    public void testSetRowKey() {
        String rowKey = "newKey";
        instance.setRowKey(rowKey);

        Assert.assertEquals(instance.getRowKey(), rowKey);
        // reset the key
        instance.setRowKey(KEY);
    }

    /**
     * Test of getColumnFamilies method, of class GenericDBTableRow.
     */
    @Test
    public void testGetColumnFamilies() {
        Map<String, ColumnFamily> result = instance.getColumnFamilies();
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    /**
     * Test of toString method, of class GenericDBTableRow.
     */
    @Test
    public void testToString() {
        String result = instance.toString();
        Assert.assertNotNull(result);
    }

}
