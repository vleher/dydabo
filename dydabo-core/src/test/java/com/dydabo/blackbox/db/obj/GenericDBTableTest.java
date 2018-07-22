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
package com.dydabo.blackbox.db.obj;

import com.dydabo.blackbox.db.obj.GenericDBTableRow.ColumnFamily;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author viswadas leher
 */
public class GenericDBTableTest {

    /**
     *
     */
    protected static final String KEY = "key";
    private static GenericDBTableRow instance;

    /**
     *
     */
    public GenericDBTableTest() {
        instance = new GenericDBTableRow(KEY);
    }

    /**
     * @throws Exception
     */
    @BeforeAll
    public static void setUpClass() throws Exception {
        instance.createFamily("default");
        GenericDBTableRow.ColumnFamily fam = instance.getColumnFamilies().get("default");
        fam.addColumn("col1", "colVal1");
    }




    /**
     * Test of getDefaultFamily method, of class GenericDBTableRow.
     */
    @Test
    public void testGetDefaultFamily() {
        GenericDBTableRow.ColumnFamily result = instance.getDefaultFamily();
        assertNotNull(result);
    }

    /**
     * Test of createFamily method, of class GenericDBTableRow.
     */
    @Test
    public void testCreateFamily() {
        String familyName = "testFamily";
        GenericDBTableRow.ColumnFamily result = instance.createFamily(familyName);
        assertNotNull(result);
    }

    /**
     * Test of getRowKey method, of class GenericDBTableRow.
     */
    @Test
    public void testGetRowKey() {
        String result = instance.getRowKey();
        assertEquals(result, KEY);
    }

    /**
     * Test of setRowKey method, of class GenericDBTableRow.
     */
    @Test
    public void testSetRowKey() {
        String rowKey = "newKey";
        instance.setRowKey(rowKey);

        assertEquals(instance.getRowKey(), rowKey);
        // reset the key
        instance.setRowKey(KEY);
    }

    /**
     * Test of getColumnFamilies method, of class GenericDBTableRow.
     */
    @Test
    public void testGetColumnFamilies() {
        Map<String, ColumnFamily> result = instance.getColumnFamilies();
        assertNotNull(result);
        assertTrue(result.size() > 0);
    }

    /**
     * Test of toString method, of class GenericDBTableRow.
     */
    @Test
    public void testToString() {
        String result = instance.toString();
        assertNotNull(result);
    }

}
