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
package com.dydabo.blackbox.hbase;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxFactory;
import com.dydabo.blackbox.beans.User;
import com.dydabo.blackbox.hbase.utils.DyDaBoTestUtils;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class PerformaceTest {

    private static DyDaBoTestUtils utils;
    private static BlackBox instance;

    public PerformaceTest() throws IOException {
        this.utils = new DyDaBoTestUtils();
        this.instance = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);
    }

    @Test
    public void testPerformanceOne() {
        try {
            List<User> users = utils.generateUsers(2000);
            instance.update(users);
            instance.fetch(users);
            instance.delete(users);
        } catch (BlackBoxException ex) {
            Logger.getLogger(PerformaceTest.class.getName()).log(Level.SEVERE, null, ex);
            Assert.fail("Performace Test Failed to execute");
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception {

    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @BeforeMethod
    public void setUpMethod() throws Exception {
    }

    @AfterMethod
    public void tearDownMethod() throws Exception {
    }

}
