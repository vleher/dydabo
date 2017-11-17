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
package com.dydabo.blackbox.usecase.company;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.usecase.company.beans.Customer;
import com.dydabo.blackbox.usecase.company.beans.Employee;
import com.dydabo.blackbox.usecase.company.beans.User;
import com.dydabo.blackbox.utils.DyDaBoTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author viswadas leher
 */
public abstract class SimpleUseCase {

    private static final Logger logger = Logger.getLogger(SimpleUseCase.class.getName());

    protected final DyDaBoTestUtils utils = new DyDaBoTestUtils();
    protected final Random random = new Random();


    /**
     * @throws IOException
     */
    public SimpleUseCase() throws IOException {

    }

    protected void insertionTest(int testSize, BlackBox blackBox) {
        List<Customer> customers = utils.generateCustomers(testSize);

        try {
            boolean flag = blackBox.insert(customers);
            assertTrue(flag);
        } catch (BlackBoxException e) {
            fail(e.getMessage(), e);
        }

        // do some fetch to verify that they are inserted
        for (Customer customer : customers) {
            try {
                List<Customer> result = blackBox.fetch(customer.getBBRowKey(), new Customer(null, null));
                assertTrue(result.size() == 1, result.size() + "");
                assertTrue(customer.getBBRowKey().equals(result.get(0).getBBRowKey()));
                assertTrue(customer.getUserName().equals(result.get(0).getUserName()));
            } catch (BlackBoxException e) {
                fail(e.getMessage(), e);
            }
        }
    }

    protected void updateTest(int testSize, BlackBox blackBox) {
        List<Customer> customers = utils.generateCustomers(testSize);
        try {
            boolean flag = blackBox.update(customers);
            assertTrue(flag);

            for (Customer customer : customers) {
                List<Customer> results = blackBox.fetch(customer.getBBRowKey(), new Customer(null, null));

                assertTrue(results.size() == 1, results.size() + "");
            }
        } catch (BlackBoxException e) {
            fail(e);
        }

    }

    protected void deleteTest(int testSize, BlackBox blackBox) {
        List<Customer> customers = utils.generateCustomers(testSize);

        // insert first...and then delete
        try {
            boolean flag = blackBox.update(customers);
            if (flag) {
                flag = blackBox.delete(customers);
                assertTrue(flag);
            }
        } catch (BlackBoxException e) {
            fail(e);
        }

    }

    protected void fetchPartialKey(int testSize, BlackBox blackBox) {
        List<Customer> customers = utils.generateCustomers(testSize);

        try {
            boolean flag = blackBox.update(customers);
            if (flag) {
                for (Customer customer : customers) {
                    final String rowKey = customer.getBBRowKey();
                    String key = rowKey.substring(0, rowKey.length() / 2) + ".*";

                    List<Customer> results = blackBox.fetchByPartialKey(key, new Customer(null, null));
                    assertTrue(results.size() > 0, results.size() + "");
                }
            }
        } catch (BlackBoxException e) {
            fail(e.getMessage(), e);
        }
    }

    protected void searchTestByName(int testSize, BlackBox blackBox) {
        List<Customer> customers = utils.generateCustomers(testSize);

        try {
            boolean flag = blackBox.update(customers);
            if (flag) {
                for (Customer customer : customers) {
                    Customer sCust = new Customer(null, customer.getUserName());
                    List<Customer> results = blackBox.search(sCust);
                    assertTrue(results.size() > 0);
                    for (Customer result : results) {
                        assertEquals(result.getUserName(), sCust.getUserName());
                    }
                }
            }
        } catch (BlackBoxException e) {
            fail(e);
        }
    }

    protected void searchMultipleTypes(int testSize, BlackBox blackBox) {
        // Update new Users
        List userList = utils.generateEmployees(testSize);
        userList.addAll(utils.generateCustomers(testSize));
        // select random key
        String userName = ((User) userList.get(random.nextInt(99) % userList.size())).getUserName();

        // Search
        List<BlackBoxable> searchList = new ArrayList<>();
        searchList.add(new Employee(null, userName));
        searchList.add(new Customer(null, userName));
        try {
            boolean success = blackBox.update(userList);
            assertTrue(success);

            List<BlackBoxable> searchResult = blackBox.search(searchList);

            assertTrue(searchResult.size() > 0);
            for (BlackBoxable res : searchResult) {
                if (res instanceof User) {
                    final String uName = ((User) res).getUserName();
                    if (uName == null || !uName.contains(userName)) {
                        fail("Does not contain  " + userName + " :" + res);
                    }
                }
            }
        } catch (Exception e) {
            fail(e);
        }
    }

    protected void searchWithWildCards(int testSize, BlackBox blackBox) {
        try {
            List<Employee> searchResults = blackBox.search(new Employee(null, null), testSize);
            if (searchResults.size() <= 0) {
                List<Employee> userList = utils.generateEmployees(testSize);
                blackBox.update(userList);
                searchResults = blackBox.search(new Employee(null, null), testSize);
            }

            String userName = searchResults.get(random.nextInt(9999) % testSize).getUserName();
            List<User> searchList = new ArrayList<>();
            final String userPrefix = userName.substring(0, 3);
            searchList.add(new Employee(null, "^" + userPrefix + ".*"));
            searchList.add(new Customer(null, "^" + userPrefix + ".*"));

            List<BlackBoxable> searchResult = blackBox.search(searchList);
            assertTrue(searchResult.size() > 0);
            for (BlackBoxable res : searchResult) {
                if (res instanceof User) {
                    final String uName = ((User) res).getUserName();
                    if (uName == null || !uName.startsWith(userPrefix)) {
                        fail(" Does not start with  " + userPrefix + " :" + res);
                    }
                }
            }
        } catch (Exception e) {
            fail(e.getMessage(), e);
        }
    }

    protected void searchWithDouble(int testSize, BlackBox blackBox) {
        final String name = "ZZZZZZZZZZZZZZZZZ";
        final int id = random.nextInt(1000);
        final double taxRate = random.nextDouble() * 100;

        Customer cust = new Customer(id, name);
        cust.setTaxRate(taxRate);

        try {
            blackBox.update(cust);
            Customer searchCust = new Customer(null, name);

            List<Customer> searchResult = blackBox.search(searchCust);
            assertTrue(!searchResult.isEmpty());
            for (Customer customer : searchResult) {
                assertTrue(customer.getUserName().equals(name), customer.getUserName());
            }

            searchCust = new Customer(id, null);

            searchResult = blackBox.search(searchCust);
            assertTrue(!searchResult.isEmpty());
            for (Customer customer : searchResult) {
                assertTrue(customer.getUserId().equals(id), customer.getUserId().toString());
            }

            searchCust = new Customer(id, name);
            searchCust.setTaxRate(taxRate);

            searchResult = blackBox.search(searchCust);
            assertTrue(!searchResult.isEmpty());
            for (Customer customer : searchResult) {
                assertTrue(customer.getTaxRate().equals(taxRate), customer.getTaxRate().toString());
            }
        } catch (BlackBoxException e) {
            fail(e.getMessage(), e);
        }
    }

    protected void rangeSearchDouble(int testSize, BlackBox blackBox) {
        List<Customer> userList = utils.generateCustomers(testSize);
        try {
            blackBox.update(userList);
            // Search tax rates
            Double tRate = userList.get(random.nextInt(99) % userList.size()).getTaxRate();
            Double minTaxRate = tRate * 0.5;
            Double maxTaxRate = tRate + minTaxRate;
            Customer startCustomer = new Customer(null, null);
            startCustomer.setTaxRate(minTaxRate);
            Customer endCustomer = new Customer(null, null);
            endCustomer.setTaxRate(maxTaxRate);
            List<Customer> taxRateCust = blackBox.search(startCustomer, endCustomer);
            assertTrue(taxRateCust.size() > 0);
            for (Customer customer : taxRateCust) {
                if (customer.getTaxRate() != null) {
                    assertTrue(customer.getTaxRate() >= minTaxRate, "Min :" + minTaxRate + " :" + customer.getTaxRate());
                    assertTrue(customer.getTaxRate() < maxTaxRate, "Max :" + maxTaxRate + " :" + customer.getTaxRate());
                }
            }

        } catch (BlackBoxException e) {
            fail(e.getMessage(), e);
        }

    }
}
