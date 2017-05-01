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
package com.dydabo.blackbox.hbase;

import org.apache.hadoop.hbase.client.Connection;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.testng.Assert.fail;

/**
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseBlackBoxImplNGTest {

    private final Logger logger = Logger.getLogger(HBaseBlackBoxImplNGTest.class.getName());

    /**
     *
     */
    public HBaseBlackBoxImplNGTest() {
    }

    /**
     * @throws Exception
     */
    @BeforeClass
    public static void setUpClass() throws Exception {
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
     * Test of getConfig method, of class HBaseBlackBoxImpl.
     */
    @Test
    public void testGetConnnection() {
        try {
            HBaseBlackBoxImpl instance = new HBaseBlackBoxImpl();
            Connection result = instance.getConnection();
            Assert.assertNotNull(result);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
            fail(ex.getMessage(), ex);
        }
    }

}
