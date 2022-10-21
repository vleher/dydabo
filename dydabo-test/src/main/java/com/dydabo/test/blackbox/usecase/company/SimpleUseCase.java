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
package com.dydabo.test.blackbox.usecase.company;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.test.blackbox.utils.DyDaBoTestUtils;

/**
 * @author viswadas leher
 */
public abstract class SimpleUseCase {

	protected final DyDaBoTestUtils utils = new DyDaBoTestUtils();
	protected final Random random = new Random();
	private final Logger logger = LogManager.getLogger();

	/**
	 * @throws IOException
	 */
	public SimpleUseCase() throws IOException {

	}

	protected void insertionTest(final int testSize, final BlackBox<Customer> blackBox) {
		final List<Customer> customers = utils.generateCustomers(testSize);

		try {
			final boolean flag = blackBox.insert(customers);
			assertTrue(flag);
		} catch (final BlackBoxException e) {
			fail(e.getMessage(), e);
		}

		// do some fetch to verify that they are inserted
		for (final Customer customer : customers) {
			try {
				final Customer c = new Customer();
				c.setUserId(customer.getUserId());
				c.setFirstName(customer.getFirstName());
				c.setLastName(customer.getLastName());
				final List<Customer> result = blackBox.fetch(c);
				assertEquals(result.size(), 1, result.size() + ":" + customer);
				assertEquals(result.get(0).getBBRowKey(), customer.getBBRowKey());
				assertEquals(result.get(0).getUserName(), customer.getUserName());
			} catch (final BlackBoxException e) {
				fail(e.getMessage(), e);
			}
		}
	}

	protected void updateTest(final int testSize, final BlackBox<Customer> blackBox) {
		final List<Customer> customers = utils.generateCustomers(testSize);
		try {
			final boolean flag = blackBox.update(customers);
			assertTrue(flag);

			for (final Customer customer : customers) {
				final Customer c = new Customer();
				c.setUserId(customer.getUserId());
				c.setFirstName(customer.getFirstName());
				c.setLastName(customer.getLastName());
				final List<Customer> results = blackBox.fetch(c);

				assertEquals(results.size(), 1, results.size() + ":" + customer.getUserName());
			}
		} catch (final BlackBoxException e) {
			fail(e);
		}

	}

	protected void deleteTest(final int testSize, final BlackBox<Customer> blackBox) {
		final List<Customer> customers = utils.generateCustomers(testSize);

		// insert first...and then delete
		try {
			boolean flag = blackBox.update(customers);
			assertTrue(flag);
			if (flag) {
				flag = blackBox.delete(customers);
				assertTrue(flag);
			}
		} catch (final BlackBoxException e) {
			fail(e);
		}
	}

	protected void fetchPartialKey(final int testSize, final BlackBox<Customer> blackBox) {
		final List<Customer> customers = utils.generateCustomers(testSize);

		try {
			final boolean flag = blackBox.update(customers);
			if (flag) {
				for (final Customer customer : customers) {
					final Customer cust = new Customer();
					cust.setUserId(customer.getUserId());
					final List<Customer> results = blackBox.fetchByPartialKey(cust);
					assertTrue(results.size() > 0, results.size() + ":" + customer.getUserId());
				}
			}
		} catch (final BlackBoxException e) {
			fail(e.getMessage(), e);
		}
	}

	protected void searchTestByName(final int testSize, final BlackBox<Customer> blackBox) {
		final List<Customer> customers = utils.generateCustomers(testSize);

		try {
			final boolean flag = blackBox.update(customers);
			if (flag) {
				for (final Customer customer : customers) {
					final Customer sCust = new Customer();
					sCust.setLastName(customer.getLastName());
					final List<Customer> results = blackBox.search(sCust);
					assertTrue(results.size() > 0);
					logger.info("Results: {}", results.size());
					for (final Customer result : results) {
						assertEquals(customer.getLastName(), result.getLastName());
					}
				}
			}
		} catch (final BlackBoxException e) {
			fail(e);
		}
	}

	protected void searchMultipleTypes(final int testSize, final BlackBox<Employee> employeeBlackBox,
			final BlackBox<Customer> custBlackBox, final BlackBox<User> userBlackBox) {
		// Update new Users
		final List<Employee> employeeList = utils.generateEmployees(testSize);
		final List<Customer> customerList = utils.generateCustomers(testSize);

		// select random key
		final String userName = customerList.get(random.nextInt(99) % customerList.size()).getUserName();

		// Search
		final List<User> searchList = new ArrayList<>();
		final Employee e1 = new Employee();
		e1.setUserName(userName);
		searchList.add(e1);
		final Customer c1 = new Customer();
		c1.setUserName(userName);
		searchList.add(c1);
		try {
			boolean success = employeeBlackBox.update(employeeList);
			assertTrue(success);

			success = custBlackBox.update(customerList);
			assertTrue(success);

			final List<User> searchResult = userBlackBox.search(searchList);

			assertTrue(searchResult.size() > 0);
			for (final User res : searchResult) {
				final String uName = res.getUserName();
				if ((uName == null) || !uName.contains(userName)) {
					fail("Does not contain  " + userName + " :" + res);
				}
			}
		} catch (final Exception e) {
			fail(e);
		}
	}

	protected void searchWithWildCards(final int testSize, final BlackBox<Employee> blackBox,
			final BlackBox<User> userBlackBox) {
		try {
			List<Employee> searchResults = blackBox.search(new Employee(), testSize, false);
			if (searchResults.size() <= testSize) {
				final List<Employee> userList = utils.generateEmployees(testSize - searchResults.size());
				blackBox.update(userList);
				searchResults = blackBox.search(new Employee(), testSize, false);
			}

			final String userName = searchResults.get(random.nextInt(9999) % testSize).getUserName();
			final List<User> searchList = new ArrayList<>();
			final String userPrefix = userName.substring(0, 3);
			final Employee e = new Employee();
			e.setUserName("^" + userPrefix + ".*");
			searchList.add(e);
			final Customer c1 = new Customer();
			c1.setUserName("^" + userPrefix + ".*");
			searchList.add(c1);

			final List<User> searchResult = userBlackBox.search(searchList);
			assertTrue(searchResult.size() > 0);
			for (final User res : searchResult) {
				final String uName = res.getUserName();
				if ((uName == null) || !uName.startsWith(userPrefix)) {
					fail(" Does not start with  " + userPrefix + " :" + res);
				}

			}
		} catch (final Exception e) {
			fail(e.getMessage(), e);
		}
	}

	protected void searchWithDouble(final int testSize, final BlackBox<Customer> blackBox) {
		final String name = "ZZZZZZZZZZZZZZZZZ";
		final int id = random.nextInt(1000);
		final double taxRate = random.nextDouble() * 100;

		final Customer cust = new Customer();
		cust.setUserId(id);
		cust.setUserName(name);
		cust.setTaxRate(taxRate);

		try {
			blackBox.update(cust);
			Customer searchCust = new Customer();
			searchCust.setUserName(name);

			List<Customer> searchResult = blackBox.search(searchCust);
			assertTrue(!searchResult.isEmpty());
			for (final Customer customer : searchResult) {
				assertEquals(name, customer.getUserName(), customer.toString());
			}

			searchCust = new Customer();
			searchCust.setUserId(id);

			searchResult = blackBox.search(searchCust);
			assertTrue(!searchResult.isEmpty());
			for (final Customer customer : searchResult) {
				assertEquals(id, (int) customer.getUserId(), customer.getUserId().toString());
			}

			searchCust = new Customer();
			searchCust.setUserId(id);
			searchCust.setUserName(name);
			searchCust.setTaxRate(taxRate);

			searchResult = blackBox.search(searchCust);
			assertTrue(!searchResult.isEmpty());
			for (final Customer customer : searchResult) {
				assertTrue(customer.getTaxRate().equals(taxRate), customer.getTaxRate().toString());
			}
		} catch (final BlackBoxException e) {
			fail(e.getMessage(), e);
		}
	}

	protected void rangeSearchDouble(final int testSize, final BlackBox<Customer> blackBox) {
		final List<Customer> userList = utils.generateCustomers(testSize);
		try {
			blackBox.update(userList);
			// Search tax rates
			final Double tRate = userList.get(random.nextInt(99) % userList.size()).getTaxRate();
			final Double minTaxRate = tRate * 0.5;
			final Double maxTaxRate = tRate + minTaxRate;
			final Customer startCustomer = new Customer();
			startCustomer.setTaxRate(minTaxRate);
			final Customer endCustomer = new Customer();
			endCustomer.setTaxRate(maxTaxRate);
			final List<Customer> taxRateCust = blackBox.search(startCustomer, endCustomer);
			assertTrue(taxRateCust.size() > 0);
			for (final Customer customer : taxRateCust) {
				if (customer.getTaxRate() != null) {
					assertTrue(customer.getTaxRate() >= minTaxRate,
							"Min :" + minTaxRate + " :" + customer.getTaxRate());
					assertTrue(customer.getTaxRate() < maxTaxRate, "Max :" + maxTaxRate + " :" + customer.getTaxRate());
				}
			}

		} catch (final BlackBoxException e) {
			fail(e.getMessage(), e);
		}

	}
}
