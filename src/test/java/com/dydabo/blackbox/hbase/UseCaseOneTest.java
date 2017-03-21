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
import com.dydabo.blackbox.beans.Employee;
import com.dydabo.blackbox.beans.User;
import com.dydabo.blackbox.hbase.utils.DyDaBoTestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
    DyDaBoTestUtils utils = new DyDaBoTestUtils();

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
        List<User> userList = utils.generateUsers(2);
        boolean success = instance.update(userList);
        Assert.assertTrue(success);

        // Delete Users
        userList = utils.generateUsers(10);
        success = instance.delete(userList);
        Assert.assertTrue(success);
    }

    @Test
    public void testUseCaseTwo() throws BlackBoxException {
        // Update 100 new Users
        List<Employee> userList = utils.generateEmployees(2);
        boolean success = instance.update(userList);
        Assert.assertTrue(success);

        // Search
        List<BlackBoxable> eList = new ArrayList();
        eList.add(new Employee(null, "Dav.*"));
        eList.add(new User(null, "Dav.*"));

        List<BlackBoxable> searchResult = instance.fetch(eList);

        System.out.println("Results:" + searchResult);

        for (BlackBoxable res : searchResult) {
            if (res instanceof User) {
                final String uName = ((User) res).getUserName();
                if (!uName.startsWith("Dav")) {
                    Assert.fail(" Does not start with Dav " + res);
                }
            } else if (res instanceof Employee) {
                final String eName = ((Employee) res).getEmployeeName();
                System.out.println("" + eName + " :" + eName.startsWith("Dav"));
                if (!eName.startsWith("Dav")) {
                    Assert.fail(" Does not start with Dav " + res);
                }
            }
        }

        // TODO: Search tax rates
        // Delete Users
        userList = utils.generateEmployees(10);
        success = instance.delete(userList);
        Assert.assertTrue(success);
    }

}
