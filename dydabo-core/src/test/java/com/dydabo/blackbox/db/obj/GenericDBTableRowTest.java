package com.dydabo.blackbox.db.obj;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author viswadas leher
 */
public class GenericDBTableRowTest {
    private GenericDBTableRow instance;

    @BeforeEach
    public void setUp() throws Exception {
        instance = new GenericDBTableRow("test-example");
    }

    @Test
    public void testGetColumnFamily() throws Exception {
        GenericDBTableRow.ColumnFamily result = instance.getColumnFamily("test-family");

        assertTrue(result != null);
    }

    @Test
    public void testGetDefaultFamily() throws Exception {
        GenericDBTableRow.ColumnFamily result = instance.getDefaultFamily();

        assertEquals(result.getFamilyName(), "D");
    }

    @Test
    public void testCreateFamily() throws Exception {
        GenericDBTableRow.ColumnFamily result = instance.createFamily("family-test");

        assertEquals(result.getFamilyName(), "family-test");
    }

    @Test
    public void testGetRowKey() throws Exception {
        assertEquals(instance.getRowKey(), "test-example");
    }

    @Test
    public void testSetRowKey() throws Exception {
        instance.setRowKey("new-rowkey");
        assertEquals(instance.getRowKey(), "new-rowkey");
        instance.setRowKey("example");
        assertEquals(instance.getRowKey(), "example");
    }

    @Test
    public void testGetColumnFamilies() throws Exception {
        Map<String, GenericDBTableRow.ColumnFamily> result = instance.getColumnFamilies();

        assertTrue(result.size() > 0);
    }

    @Test
    public void testToJsonObject() throws Exception {
        JsonObject result = instance.toJsonObject();
        assertNotNull(result);

        instance.getColumnFamily("testfamily");

        result = instance.toJsonObject();
        assertNotNull(result);
    }

    @Test
    public void testForEach() throws Exception {
        //Object result = instance.forEach("familyname", "col1", "col2", "col3");
    }

    @Test
    public void testToString() throws Exception {
        assertNotNull(instance.toString());
    }
}