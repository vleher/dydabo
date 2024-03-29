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
package com.dydabo.blackbox.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.dydabo.blackbox.common.utils.DyDaBoUtils;

/**
 * @author viswadas leher
 */
public class DyDaBoUtilsTest {

	static Stream<Arguments> stringAndNullProvider() {
		return Stream.of(Arguments.of(true, new String[] { "foo", "", "test" }),
				Arguments.of(true, new String[] { null, "bar", "23" }),
				Arguments.of(false, new String[] { "qqqq", "bar", "23" }));
	}

	/**
	 *
	 */
	@Test
	public void testConstructor() {
		final DyDaBoUtils result = new DyDaBoUtils();
		assertNotNull(result);
	}

	/**
	 * Test of isBlankOrNull method, of class DyDaBoUtils.
	 *
	 * @param expResult
	 * @param str
	 */
	@ParameterizedTest
	@MethodSource("stringAndNullProvider")
	public void testIsBlankOrNull(final boolean expResult, final String... str) {
		assertEquals(DyDaBoUtils.isBlankOrNull(str), expResult);
	}

	/**
	 * Test of isNotBlankOrNull method, of class DyDaBoUtils.
	 *
	 * @param expResult
	 * @param str
	 */
	@ParameterizedTest
	@MethodSource("stringAndNullProvider")
	public void testIsNotBlankOrNull(final boolean expResult, final String... str) {
		assertEquals(DyDaBoUtils.isNotBlankOrNull(str), !expResult);
	}

	/**
	 * Test of isValidRegex method, of class DyDaBoUtils.
	 */
	@Test
	public void testIsValidRegex() {
		assertFalse(DyDaBoUtils.isValidRegex(""));
		assertTrue(DyDaBoUtils.isValidRegex(".*"));
		assertFalse(DyDaBoUtils.isValidRegex(null));
		assertFalse(DyDaBoUtils.isValidRegex("[]"));
		assertFalse(DyDaBoUtils.isValidRegex("{}"));
	}

	/**
	 * Test of parseJsonString method, of class DyDaBoUtils.
	 */
	@Test
	public void testParseJsonString() {
		assertNull(DyDaBoUtils.parseJsonString(null));
		assertNull(DyDaBoUtils.parseJsonString("{"));
		assertNull(DyDaBoUtils.parseJsonString("{]sss"));
	}

	/**
	 * Test of getStringPrefix method, of class DyDaBoUtils.
	 */
	@Test
	public void testGetStringPrefix() {
		assertEquals(DyDaBoUtils.getStringPrefix("abcd.*"), "abcd");
		assertEquals(DyDaBoUtils.getStringPrefix(".*abcd"), "");
		assertEquals(DyDaBoUtils.getStringPrefix("[abcd.*"), "");
		assertEquals(DyDaBoUtils.getStringPrefix("12.*ret[avc]"), "12");
		assertEquals(DyDaBoUtils.getStringPrefix("[12].*ret[avc]"), "");
		assertEquals(DyDaBoUtils.getStringPrefix("MSIE.((5\\.[5-9])|([6-9]|1[0-9]))"), "MSIE");
	}

	/**
	 * Test of isPrimitiveOrPrimitiveWrapperOrString method, of class DyDaBoUtils.
	 */
	@Test
	public void testIsPrimitiveOrPrimitiveWrapperOrString() {
		assertTrue(DyDaBoUtils.isPrimitiveOrPrimitiveWrapperOrString(Double.MAX_VALUE));
		assertTrue(DyDaBoUtils.isPrimitiveOrPrimitiveWrapperOrString("abcd"));
		assertTrue(DyDaBoUtils.isPrimitiveOrPrimitiveWrapperOrString(Integer.max(2, 6)));
		assertTrue(DyDaBoUtils.isPrimitiveOrPrimitiveWrapperOrString(34.9f));
		assertFalse(DyDaBoUtils.isPrimitiveOrPrimitiveWrapperOrString(DyDaBoUtils.class));
		assertFalse(DyDaBoUtils.isPrimitiveOrPrimitiveWrapperOrString(new Patient()));
		assertFalse(DyDaBoUtils.isPrimitiveOrPrimitiveWrapperOrString(null));
	}

	@Test
	public void testIsARegex() throws Exception {
		assertTrue(DyDaBoUtils.isARegex("Tom.*"));
		assertTrue(DyDaBoUtils.isARegex("Tom."));
		assertTrue(DyDaBoUtils.isARegex("^Tom"));
		assertTrue(DyDaBoUtils.isARegex("^Tom.*"));
		assertFalse(DyDaBoUtils.isARegex(null));
		assertFalse(DyDaBoUtils.isARegex("ZZZz"));
	}

	@Test
	public void testIsNumber() throws Exception {
		assertTrue(DyDaBoUtils.isNumber(Double.valueOf("555.0")));
		// assertTrue(DyDaBoUtils.isNumber(Integer.getInteger("3")));
		assertFalse(DyDaBoUtils.isNumber(null));
	}

	/**
	 *
	 */
	@Test
	public void testGetFieldFromType() {
		final Customer cust = new Customer();
		final Map<String, Field> result = DyDaBoUtils.getFieldFromType(cust.getClass());

		assertTrue(result.size() > 0);

		final Field nextResult = DyDaBoUtils.getFieldFromType(cust.getClass(), "userName");
		assertTrue(nextResult != null);
	}
}
