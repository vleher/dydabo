package com.dydabo.blackbox.hbase.utils;

import com.dydabo.blackbox.beans.SampleBean;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author viswadas leher
 */
public class HBaseUtilsTest {

    HBaseUtils hbaseUtils;
    private SampleBean testRow;

    @BeforeMethod
    public void setUp() throws Exception {
        hbaseUtils = new HBaseUtils();
        testRow = new SampleBean("example");
    }

    @Test
    public void testIsValidRowKey() throws Exception {
        Assert.assertEquals(hbaseUtils.isValidRowKey(null), false);
        Assert.assertEquals(hbaseUtils.isValidRowKey(testRow), true);
    }

}