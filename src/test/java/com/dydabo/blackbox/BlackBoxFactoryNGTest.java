/*
 * Copyright (C) 2017 viswadas leher <vleher@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.dydabo.blackbox;

import com.dydabo.blackbox.hbase.HBaseJsonImpl;
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

    public BlackBoxFactoryNGTest() {
    }

    @org.testng.annotations.BeforeClass
    public static void setUpClass() throws Exception {
    }

    @org.testng.annotations.AfterClass
    public static void tearDownClass() throws Exception {
    }

    @org.testng.annotations.BeforeMethod
    public void setUpMethod() throws Exception {
    }

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
            Assert.assertTrue(result instanceof HBaseJsonImpl);

            result = BlackBoxFactory.getDatabase("Dummy");
            Assert.assertNull(result);
        } catch (IOException ex) {
            Logger.getLogger(BlackBoxFactoryNGTest.class.getName()).log(Level.SEVERE, null, ex);
            Assert.fail(ex.getMessage(), ex);
        }

    }

    /**
     * Test of getHBaseDatabase method, of class BlackBoxFactory.
     */
    @Test
    public void testGetHBaseDatabase() throws Exception {
        System.out.println("getHBaseDatabase");
        Configuration config = HBaseConfiguration.create();
        BlackBox result = BlackBoxFactory.getHBaseDatabase(config);
        Assert.assertTrue(result instanceof HBaseJsonImpl);
    }

    @Test
    public void testConstructorIsPrivate() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<BlackBoxFactory> constructor = BlackBoxFactory.class.getDeclaredConstructor();
        org.junit.Assert.assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        constructor.newInstance();
    }
}
