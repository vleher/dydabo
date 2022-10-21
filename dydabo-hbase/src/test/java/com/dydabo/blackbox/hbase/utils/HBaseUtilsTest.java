package com.dydabo.blackbox.hbase.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.test.blackbox.beans.SampleBean;

/**
 * @author viswadas leher
 */
public class HBaseUtilsTest {

	HBaseUtils<SampleBean> hbaseUtils;
	private SampleBean testRow;

	@BeforeEach
	public void setUp() throws Exception {
		hbaseUtils = new HBaseUtils<>();
		testRow = new SampleBean("example");
	}

	@Test
	public void testIsValidRowKey() throws Exception {
		assertEquals(hbaseUtils.isValidRowKey(null), false);
		assertEquals(hbaseUtils.isValidRowKey(testRow), true);
	}

	@Test
	public void convertRowToTableRow() {
		final GenericDBTableRow result = hbaseUtils.convertRowToTableRow(testRow);

		Assertions.assertEquals(result.getRowKey(), "example");
	}
}
