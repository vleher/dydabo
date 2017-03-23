/*
 * Copyright (C) 2017 leher
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
package com.dydabo.blackbox.beans;

import com.dydabo.blackbox.BlackBoxable;
import com.google.gson.Gson;
import java.util.UUID;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author leher
 */
public class Employee extends User implements BlackBoxable {

    private String employeeTitle;
    private Integer salary;

    public Employee(Integer id, String name) {
        super(id, name);
    }

    public String getEmployeeTitle() {
        return employeeTitle;
    }

    public void setEmployeeTitle(String employeeTitle) {
        this.employeeTitle = employeeTitle;
    }

    public Integer getSalary() {
        return salary;
    }

    public void setSalary(Integer salary) {
        this.salary = salary;
    }

    @Override
    public String getBBRowKey() {
        // user name is required, and so is user id
        if (getUserName() != null) {
            UUID uniqueId = UUID.nameUUIDFromBytes(Bytes.toBytes(getUserName() + getUserId()));
            return uniqueId.toString();
        }
        return "";
    }

    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return "Employee{ userID=" + getUserId() + ", UserName=" + getUserName() + ", employeeTitle=" + employeeTitle + ", salary=" + salary + '}';
    }

}
