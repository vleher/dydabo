package com.dydabo.blackbox.db.obj;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author viswadas leher
 */
public class GenericDBTRColumnTest {
    private GenericDBTableRow.Column column;

    @BeforeEach
    public void setUp() throws Exception {
        GenericDBTableRow tableRow = new GenericDBTableRow("rowKey");
        tableRow.createFamily("family1").addColumn("col1", "colvalue1");
        column = tableRow.getColumnFamily("family1").getColumn("col1");
    }

    @Test
    public void testGetColumnName() throws Exception {
        assertEquals(column.getColumnName(), "col1");
    }

    @Test
    public void testSetColumnName() throws Exception {
        column.setColumnName("col2");
        assertEquals(column.getColumnName(), "col2");
    }

    @Test
    public void testGetColumnValue() throws Exception {
        assertEquals(column.getColumnValue(), "colvalue1");
    }

    @Test
    public void testSetColumnValue() throws Exception {
        column.setColumnValue("colvalue2");
        assertEquals(column.getColumnValue(), "colvalue2");
    }

    @Test
    public void testGetColumnValueAsString() throws Exception {
        assertEquals(column.getColumnValueAsString(), "colvalue1");

        column.setColumnValue(null);
        assertNull(column.getColumnValueAsString());

        column.setColumnValue(12);
        assertEquals(column.getColumnValueAsString(), "12");

        column.setColumnValue(new String[]{"c1", "c2", "c3"});
        assertNotNull(column.getColumnValueAsString());
    }

    @Test
    public void testToString() throws Exception {
        assertNotNull(column.toString());
    }
}