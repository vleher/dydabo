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
package com.dydabo.blackbox.common;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class DyDaBoUtilsNGTest {

    /**
     *
     */
    public DyDaBoUtilsNGTest() {
    }

    /**
     *
     * @throws Exception
     */
    @org.testng.annotations.BeforeClass
    public static void setUpClass() throws Exception {
    }

    /**
     *
     * @throws Exception
     */
    @org.testng.annotations.AfterClass
    public static void tearDownClass() throws Exception {
    }

    /**
     *
     * @throws Exception
     */
    @org.testng.annotations.BeforeMethod
    public void setUpMethod() throws Exception {
    }

    /**
     *
     * @throws Exception
     */
    @org.testng.annotations.AfterMethod
    public void tearDownMethod() throws Exception {
    }

    /**
     *
     */
    @Test
    public void testConstructor() {
        DyDaBoUtils result = new DyDaBoUtils();
        Assert.assertNotNull(result);
    }

    /**
     * Test of isBlankOrNull method, of class DyDaBoUtils.
     * @param expResult
     * @param str
     */
    @Test(dataProvider = "stringtestdata")
    public void testIsBlankOrNull(boolean expResult, String[] str) {
        boolean result = DyDaBoUtils.isBlankOrNull(str);
        Assert.assertEquals(result, expResult);
    }

    /**
     *
     * @return
     */
    @DataProvider(name = "stringtestdata")
    public Object[][] stringTestData() {
        return new Object[][]{
            {true, new String[]{"", " ", "test"}},
            {true, new String[]{"", null, "test"}},
            {true, new String[]{"abcd", " ", "test"}},
            {false, new String[]{"axd", " test ", "test"}},
            {true, new String[]{"erwer", "werwer ", null}}
        };
    }

    /**
     * Test of isValidRegex method, of class DyDaBoUtils.
     */
    @Test
    public void testIsValidRegex() {
        Assert.assertEquals(DyDaBoUtils.isValidRegex(""), false);
        Assert.assertEquals(DyDaBoUtils.isValidRegex(".*"), true);
        Assert.assertEquals(DyDaBoUtils.isValidRegex(null), false);
        Assert.assertEquals(DyDaBoUtils.isValidRegex("[]"), false);
        Assert.assertEquals(DyDaBoUtils.isValidRegex("{}"), false);
    }

    /**
     * Test of parseJsonString method, of class DyDaBoUtils.
     */
    @Test
    public void testParseJsonString() {
        Assert.assertNull(DyDaBoUtils.parseJsonString(null));
        Assert.assertNull(DyDaBoUtils.parseJsonString("{"));
        Assert.assertNull(DyDaBoUtils.parseJsonString("{]sss"));
    }

}
