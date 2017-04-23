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
package com.dydabo.blackbox.hbase;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxFactory;
import com.dydabo.blackbox.beans.Customer;
import com.dydabo.blackbox.hbase.utils.DyDaBoTestUtils;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class PerformaceTest {

    private static DyDaBoTestUtils utils;
    private static BlackBox instance;
    private final Logger logger = Logger.getLogger(PerformaceTest.class.getName());

    /**
     *
     * @throws IOException
     */
    public PerformaceTest() throws IOException {
        PerformaceTest.utils = new DyDaBoTestUtils();
        PerformaceTest.instance = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);
    }

    /**
     *
     */
    @Test
    public void testPerformanceOne() {
        try {
            final int testSize = 10;
            // Delete all rows in table
            Customer c = new Customer(null, null);
            List<Customer> allUsers = instance.search(Collections.singletonList(c));
            if (allUsers.size() > 0) {
                instance.delete(allUsers);
            }
            // Insert new customers
            List<Customer> users = utils.generateCustomers(testSize);
            instance.update(users);

            List<Customer> userQueryList = new ArrayList<>();
            for (Customer user : users) {
                userQueryList.add(new Customer(null, user.getUserName()));
            }

            List<Customer> results = instance.search(userQueryList);

            Assert.assertTrue(results.size() > testSize);
        } catch (BlackBoxException ex) {
            logger.log(Level.SEVERE, null, ex);
            Assert.fail("Performace Test Failed to execute");
        }
    }

    /**
     *
     * @throws Exception
     */
    @BeforeClass
    public static void setUpClass() throws Exception {

    }

    /**
     *
     * @throws Exception
     */
    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    /**
     *
     * @throws Exception
     */
    @BeforeMethod
    public void setUpMethod() throws Exception {
    }

    /**
     *
     * @throws Exception
     */
    @AfterMethod
    public void tearDownMethod() throws Exception {
    }

}
