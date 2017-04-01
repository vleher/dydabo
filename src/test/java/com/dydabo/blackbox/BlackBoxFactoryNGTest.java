/** *****************************************************************************
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************
 */
package com.dydabo.blackbox;

import com.dydabo.blackbox.hbase.HBaseBlackBoxImpl;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class BlackBoxFactoryNGTest {

    private final Logger logger = Logger.getLogger(BlackBoxFactoryNGTest.class.getName());

    /**
     *
     */
    public BlackBoxFactoryNGTest() {
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
     * Test of getDatabase method, of class BlackBoxFactory.
     */
    @org.testng.annotations.Test
    public void testGetDatabase() {
        try {
            // Test Hbase
            BlackBox result = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);
            Assert.assertTrue(result instanceof HBaseBlackBoxImpl);

            result = BlackBoxFactory.getDatabase("Dummy");
            Assert.assertNull(result);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
            Assert.fail(ex.getMessage(), ex);
        }

    }

    /**
     * Test of getHBaseDatabase method, of class BlackBoxFactory.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testGetHBaseDatabase() throws Exception {
        Configuration config = HBaseConfiguration.create();
        BlackBox result = BlackBoxFactory.getHBaseDatabase(config);
        Assert.assertTrue(result instanceof HBaseBlackBoxImpl);
    }

    /**
     *
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    @Test
    public void testConstructorIsPrivate() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<BlackBoxFactory> constructor = BlackBoxFactory.class.getDeclaredConstructor();
        org.junit.Assert.assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        constructor.newInstance();
    }
}
