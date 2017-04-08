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
package com.dydabo.blackbox.usecase;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxFactory;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.beans.Customer;
import com.dydabo.blackbox.beans.Employee;
import com.dydabo.blackbox.beans.User;
import com.dydabo.blackbox.hbase.utils.DyDaBoTestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
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
public class SimpleUseCase {

    private static final Logger logger = Logger.getLogger(SimpleUseCase.class.getName());

    private final BlackBox cassandraInstance;
    private final BlackBox hbaseInstance;
    private final BlackBox mongoInstance;
    private final DyDaBoTestUtils utils = new DyDaBoTestUtils();
    private final Random random = new Random();

    /**
     *
     * @throws IOException
     */
    public SimpleUseCase() throws IOException {
        hbaseInstance = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);
        cassandraInstance = BlackBoxFactory.getCassandraDatabase();
        mongoInstance = BlackBoxFactory.getMongoDatabase();
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

    /**
     *
     * @throws BlackBoxException
     */
    @Test
    public void testUseCaseOne() throws BlackBoxException {
        int testSize = 3;
        // Update new Users
        List<Customer> userList = utils.generateCustomers(testSize);

        boolean success = hbaseInstance.update(userList);
        Assert.assertTrue(success);

        success = cassandraInstance.update(userList);
        Assert.assertTrue(success);

        success = mongoInstance.update(userList);
        Assert.assertTrue(success);

        // Delete Users
        userList = utils.generateCustomers(testSize);
        success = hbaseInstance.delete(userList);
        Assert.assertTrue(success);

        success = cassandraInstance.delete(userList);
        Assert.assertTrue(success);

        success = mongoInstance.delete(userList);
        Assert.assertTrue(success);
    }

    /**
     *
     * @throws BlackBoxException
     */
    @Test
    public void testUseCaseTwo() throws BlackBoxException {
        int testSize = 1;
        // Update new Users
        List<Employee> userList = utils.generateEmployees(testSize);
        boolean success = hbaseInstance.update(userList);
        Assert.assertTrue(success);

        success = cassandraInstance.update(userList);
        Assert.assertTrue(success);

        success = mongoInstance.update(userList);
        Assert.assertTrue(success);

        // Search
        List<BlackBoxable> hbaseList = new ArrayList();
        hbaseList.add(new Employee(null, "David"));
        hbaseList.add(new Customer(null, "David"));

        List<BlackBoxable> searchResult = hbaseInstance.search(hbaseList);
        logger.info("HBASE Search Result :" + searchResult.size());
        for (BlackBoxable res : searchResult) {
            if (res instanceof User) {
                final String uName = ((User) res).getUserName();
                if (uName == null || !uName.contains("David")) {
                    Assert.fail(" Does not contain David " + res);
                }
            }
        }

        List<BlackBoxable> cassList = new ArrayList();
        cassList.add(new Employee(null, "Harry King"));
        cassList.add(new Customer(null, "Harry King"));
        searchResult = cassandraInstance.search(cassList);
        logger.info("CASS Search Result :" + searchResult.size());
        for (BlackBoxable res : searchResult) {
            if (res instanceof User) {
                final String uName = ((User) res).getUserName();
                if (uName == null || !uName.startsWith("Harry")) {
                    Assert.fail(" Does not start with Harry " + res);
                }
            }
        }

        searchResult = mongoInstance.search(cassList);
        logger.info("Mongo Search Result :" + searchResult.size());
        for (BlackBoxable res : searchResult) {
            if (res instanceof User) {
                final String uName = ((User) res).getUserName();
                if (uName == null || !uName.startsWith("Harry")) {
                    Assert.fail(" Does not start with Harry " + res);
                }
            }
        }

        // Search
        hbaseList = new ArrayList();
        hbaseList.add(new Employee(null, "^Dav.*"));
        hbaseList.add(new Customer(null, "^Dav.*"));

        searchResult = hbaseInstance.search(hbaseList);
        logger.info("HBASE Search Result :" + searchResult.size());
        for (BlackBoxable res : searchResult) {
            if (res instanceof User) {
                final String uName = ((User) res).getUserName();
                if (uName == null || !uName.startsWith("Dav")) {
                    Assert.fail(" Does not start with Dav " + res);
                }
            }
        }

        cassList = new ArrayList();
        cassList.add(new Employee(null, "Harry.*"));
        cassList.add(new Customer(null, "Harry.*"));
        searchResult = cassandraInstance.search(cassList);
        logger.info("CASS Search Result :" + searchResult.size());
        for (BlackBoxable res : searchResult) {
            if (res instanceof User) {
                final String uName = ((User) res).getUserName();
                if (uName == null || !uName.startsWith("Harry")) {
                    Assert.fail(" Does not start with Harry " + res);
                }
            }
        }

        // Search tax rates
        Double minTaxRate = random.nextDouble() * 10;
        Double maxTaxRate = random.nextDouble() * 100 + minTaxRate;
        Customer startCustomer = new Customer(null, null);
        startCustomer.setTaxRate(minTaxRate);
        Customer endCustomer = new Customer(null, null);
        endCustomer.setTaxRate(maxTaxRate);

        List<Customer> taxRateCust = hbaseInstance.search(startCustomer, endCustomer);
        for (Customer customer : taxRateCust) {
            if (customer.getTaxRate() != null) {
                Assert.assertTrue(customer.getTaxRate() >= minTaxRate);
                Assert.assertTrue(customer.getTaxRate() < maxTaxRate);
            }
        }

        taxRateCust = cassandraInstance.search(startCustomer, endCustomer);
        for (Customer customer : taxRateCust) {
            if (customer.getTaxRate() != null) {
                Assert.assertTrue(customer.getTaxRate() >= minTaxRate);
                Assert.assertTrue(customer.getTaxRate() < maxTaxRate);
            }
        }

        // Delete Users
        userList = utils.generateEmployees(testSize);
        success = hbaseInstance.delete(userList);
        Assert.assertTrue(success);
        success = cassandraInstance.delete(userList);
        Assert.assertTrue(success);
    }

    /**
     *
     * @throws BlackBoxException
     */
    @Test
    public void testUseCaseThree() throws BlackBoxException {
        // Insert an unique record
        Customer cust = new Customer(new Random().nextInt(1000), "AQWERDSFSTOIOPIoioioiIIII1111");
        cust.setTaxRate(random.nextDouble() * 100);
        hbaseInstance.insert(cust);
        cassandraInstance.insert(cust);
        mongoInstance.insert(cust);

        Customer cust1 = new Customer(new Random().nextInt(1000), "AQWERDSFSTOIOPIoioioiIIII222");
        cust1.setTaxRate(random.nextDouble() * 100);
        hbaseInstance.insert(cust1);
        cassandraInstance.insert(cust1);
        mongoInstance.insert(cust1);

        Customer cust2 = new Customer(new Random().nextInt(1000), "AQWERDSFSTOIOPIoioioiIIII333");
        cust2.setTaxRate(random.nextDouble() * 100);
        hbaseInstance.insert(cust2);
        cassandraInstance.insert(cust2);
        mongoInstance.insert(cust2);

        List<BlackBoxable> searchResult = hbaseInstance.search(cust);
        Assert.assertEquals(searchResult.size(), 1);

        searchResult = cassandraInstance.search(cust);
        Assert.assertEquals(searchResult.size(), 1);

        searchResult = mongoInstance.search(cust);
        Assert.assertEquals(searchResult.size(), 1);

        searchResult = hbaseInstance.fetch(Arrays.asList(cust.getBBRowKey(), cust1.getBBRowKey(), cust2.getBBRowKey()), cust);
        Assert.assertEquals(searchResult.size(), 3);

        searchResult = mongoInstance.fetch(Arrays.asList(cust.getBBRowKey(), cust1.getBBRowKey(), cust2.getBBRowKey()), cust);
        Assert.assertEquals(searchResult.size(), 3);

        searchResult = cassandraInstance.fetch(Arrays.asList(cust.getBBRowKey(), cust1.getBBRowKey(), cust2.getBBRowKey()), cust);
        Assert.assertEquals(searchResult.size(), 3);

        // Try to clean up
        hbaseInstance.delete(Arrays.asList(cust, cust1, cust2));
        cassandraInstance.delete(Arrays.asList(cust, cust1, cust2));
        mongoInstance.delete(Arrays.asList(cust, cust1, cust2));
    }

    @Test
    public void testFetchPartial() throws BlackBoxException {
        final Employee employee = new Employee(null, null);
        // Update new Users
        List<Employee> userList = hbaseInstance.search(employee);

        // select random key
        String key = userList.get(random.nextInt(99) % userList.size()).getBBRowKey();

        String keyQuery1 = key.substring(0, key.length() / 2) + ".*";

        List<Employee> hbaseEmps = hbaseInstance.fetchByPartialKey(keyQuery1, employee);
        Assert.assertTrue(hbaseEmps.size() > 0);
        for (Employee emp : hbaseEmps) {
            Assert.assertTrue(emp.getBBRowKey().startsWith(key), emp.toString());
        }

        List<Employee> cassEmps = cassandraInstance.fetchByPartialKey(keyQuery1, employee);
        Assert.assertTrue(cassEmps.size() > 0);
        for (Employee emp : cassEmps) {
            Assert.assertTrue(emp.getBBRowKey().startsWith(key), emp.toString());
        }

        List<Employee> mongoEmps = mongoInstance.fetchByPartialKey(keyQuery1, employee);
        Assert.assertTrue(mongoEmps.size() > 0);
        for (Employee emp : mongoEmps) {
            Assert.assertTrue(emp.getBBRowKey().startsWith(key), emp.toString());
        }
    }

    /**
     *
     * @throws BlackBoxException
     */
    @Test
    public void testDoubleSearch() throws BlackBoxException {
        final String name = "ZZZZZZZZZZZZZZZZZ";
        final int id = random.nextInt(1000);
        final double taxRate = random.nextDouble() * 100;

        Customer cust = new Customer(id, name);
        cust.setTaxRate(taxRate);
        hbaseInstance.insert(cust);
        cassandraInstance.insert(cust);
        mongoInstance.insert(cust);

        Customer searchCust = new Customer(null, name);

        List<Customer> searchResult = mongoInstance.search(searchCust);
        Assert.assertTrue(!searchResult.isEmpty());
        for (Customer customer : searchResult) {
            Assert.assertTrue(customer.getUserName().equals(name), customer.getUserName());
        }

        searchCust = new Customer(id, null);

        searchResult = mongoInstance.search(searchCust);
        Assert.assertTrue(!searchResult.isEmpty());
        for (Customer customer : searchResult) {
            Assert.assertTrue(customer.getUserId().equals(id), customer.getUserId().toString());
        }

        searchCust = new Customer(null, name);
        searchCust.setTaxRate(taxRate);

        searchResult = mongoInstance.search(searchCust);
        Assert.assertTrue(!searchResult.isEmpty());
        for (Customer customer : searchResult) {
            Assert.assertTrue(customer.getTaxRate().equals(taxRate), customer.getTaxRate().toString());
        }

    }
}
