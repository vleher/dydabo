package com.dydabo.blackbox;

import com.dydabo.blackbox.beans.SampleBean;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author viswadas leher
 */
public class HBaseUtilsTest {

    HBaseUtils hbaseUtils;
    private SampleBean testRow;

    @BeforeEach
    public void setUp() throws Exception {
        hbaseUtils = new HBaseUtils();
        testRow = new SampleBean("example");
    }

    @Test
    public void testIsValidRowKey() throws Exception {
        assertEquals(hbaseUtils.isValidRowKey(null), false);
        assertEquals(hbaseUtils.isValidRowKey(testRow), true);
    }

    @Test
    public void convertRowToTableRow() {
        GenericDBTableRow result = hbaseUtils.convertRowToTableRow(testRow);

        Assertions.assertEquals(result.getRowKey(), "example");
    }
}
