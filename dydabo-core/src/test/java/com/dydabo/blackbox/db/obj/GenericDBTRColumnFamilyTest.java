package com.dydabo.blackbox.db.obj;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author viswadas leher
 */
public class GenericDBTRColumnFamilyTest {
	private GenericDBTableRow.ColumnFamily colFamily;

	@BeforeEach
	public void setUp() throws Exception {
		final GenericDBTableRow instance = new GenericDBTableRow("rowKey");
		colFamily = instance.getDefaultFamily();
		colFamily.addColumn("col1", 12);
	}

	@Test
	public void testGetColumn() throws Exception {
		colFamily.addColumn("col1", 12);

		assertEquals(colFamily.getColumn("col1").getColumnValue(), 12);
	}

	@Test
	public void testGetFamilyName() throws Exception {
		assertEquals(colFamily.getFamilyName(), "D");
	}

	@Test
	public void testSetFamilyName() throws Exception {
		colFamily.setFamilyName("family1");
		assertEquals(colFamily.getFamilyName(), "family1");
	}

	@Test
	public void testGetColumns() throws Exception {
		Map<String, GenericDBTableRow.Column> result = colFamily.getColumns();
		assertNotNull(result);

		colFamily.addColumn("col1", 12);
		result = colFamily.getColumns();
		assertTrue(result.size() > 0);
	}

	@Test
	public void testToJsonObject() throws Exception {
		assertNotNull(colFamily.toJsonObject());

		colFamily.addColumn("col1", "colvalue1");
		assertNotNull(colFamily.toJsonObject());

		colFamily.addColumn("col2", 12);
		assertNotNull(colFamily.toJsonObject());
	}

	@Test
	public void testToString() throws Exception {
		assertNotNull(colFamily.toString());
	}
}
