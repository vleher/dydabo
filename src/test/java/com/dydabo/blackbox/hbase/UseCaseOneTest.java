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
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.test.beans.Employee;
import com.dydabo.blackbox.test.beans.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
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
public class UseCaseOneTest {

    BlackBox instance = null;

    public UseCaseOneTest() throws IOException {
        instance = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);
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

    @Test
    public void testUseCaseOne() throws BlackBoxException {
        // Update 100 new Users
        List<User> userList = generateUsers(2);
        boolean success = instance.update(userList);
        Assert.assertTrue(success);

        // Delete Users
        userList = generateUsers(10);
        success = instance.delete(userList);
        Assert.assertTrue(success);
    }

    @Test
    public void testUseCaseTwo() throws BlackBoxException {
        // Update 100 new Users
        List<Employee> userList = generateEmployees(2);
        boolean success = instance.update(userList);
        Assert.assertTrue(success);

        // Search
        List<BlackBoxable> eList = new ArrayList();
        eList.add(new Employee(null, ".*"));
        eList.add(new User(null, ".*"));

        List searchResult = instance.fetch(eList);
        System.out.println("Results: " + searchResult);

        // Delete Users
        userList = generateEmployees(10);
        success = instance.delete(userList);
        Assert.assertTrue(success);
    }

    private List<User> generateUsers(int maxNumber) {
        List<User> userList = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < maxNumber; i++) {
            int id = random.nextInt();
            final User user = new User(id, FirstNames.get(Math.abs(id % 10)) + " " + LastNames.get(Math.abs(id % 10)));
            user.setTaxRate(random.nextDouble() * 100);
            userList.add(user);
        }
        return userList;
    }

    private List<Employee> generateEmployees(int maxNumber) {
        List<Employee> userList = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < maxNumber; i++) {
            int id = random.nextInt();
            final Employee employee = new Employee(id, FirstNames.get(Math.abs(id % 10)) + " " + LastNames.get(Math.abs(id % 10)));
            userList.add(employee);
        }
        return userList;
    }

    private List<String> FirstNames = Arrays.asList("David", "Peter", "Tom", "Dick", "Harry", "John", "Bill", "Adele", "Britney", "Mariah");
    private List<String> LastNames = Arrays.asList("Johnson", "Becker", "Smith", "Gates", "King", "Spears", "Perry", "Carey", "Gomez", "Lopez");

}
