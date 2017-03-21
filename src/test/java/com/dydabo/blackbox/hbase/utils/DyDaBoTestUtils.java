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
package com.dydabo.blackbox.hbase.utils;

import com.dydabo.blackbox.beans.Employee;
import com.dydabo.blackbox.beans.User;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class DyDaBoTestUtils {

    public List<String> FirstNames = Arrays.asList("David", "Peter", "Tom", "Dick", "Harry", "John", "Bill", "Adele", "Britney", "Mariah");
    public List<String> LastNames = Arrays.asList("Johnson", "Becker", "Smith", "Gates", "King", "Spears", "Perry", "Carey", "Gomez", "Lopez");

    public List<User> generateUsers(int maxNumber) {
        List<User> userList = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < maxNumber; i++) {
            int id = random.nextInt();
            final User user = new User(id, FirstNames.get(Math.abs(id % FirstNames.size())) + " " +
                     LastNames.get(Math.abs(id % LastNames.size())));
            user.setTaxRate(random.nextDouble() * 100);
            userList.add(user);
        }
        return userList;
    }

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
