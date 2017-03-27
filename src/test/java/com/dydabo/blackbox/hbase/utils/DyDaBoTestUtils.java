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
package com.dydabo.blackbox.hbase.utils;

import com.dydabo.blackbox.beans.Customer;
import com.dydabo.blackbox.beans.Employee;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class DyDaBoTestUtils {

    /**
     *
     */
    public List<String> FirstNames = Arrays.asList("David", "Peter", "Tom", "Dick", "Harry", "John", "Bill", "Adele", "Britney", "Mariah");

    /**
     *
     */
    public List<String> LastNames = Arrays.asList("Johnson", "Becker", "Smith", "Gates", "King", "Spears", "Perry", "Carey", "Gomez", "Lopez");

    /**
     *
     * @param maxNumber
     * @return
     */
    public List<Customer> generateCustomers(int maxNumber) {
        List<Customer> custList = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < maxNumber; i++) {
            int id = random.nextInt();
            final Customer customer = new Customer(id, FirstNames.get(Math.abs(id % FirstNames.size())) + " " +
                    LastNames.get(Math.abs(id % LastNames.size())));
            customer.setTaxRate(random.nextDouble() * 100);
            customer.initData();
            custList.add(customer);
        }
        return custList;
    }

    /**
     *
     * @param maxNumber
     * @return
     */
    public List<Employee> generateEmployees(int maxNumber) {
        List<Employee> userList = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < maxNumber; i++) {
            int id = random.nextInt();
            final Employee employee = new Employee(id, FirstNames.get(Math.abs(id % FirstNames.size())) +
                    " " + LastNames.get(Math.abs(id % LastNames.size())));
            userList.add(employee);
        }
        return userList;
    }

}
