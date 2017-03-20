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
package com.dydabo.blackbox.test.beans;

import com.dydabo.blackbox.BlackBoxable;
import com.google.gson.Gson;

/**
 *
 * @author leher
 */
public class Employee implements BlackBoxable {

    private Integer employeeId;
    private String employeeName;

    public Employee(Integer id, String name) {
        this.employeeId = id;
        this.employeeName = name;
    }

    public Integer getEmployeeId() {
        return employeeId;
    }

    public void setEmployeeId(Integer employeeId) {
        this.employeeId = employeeId;
    }

    public String getEmployeeName() {
        return employeeName;
    }

    public void setEmployeeName(String employeeName) {
        this.employeeName = employeeName;
    }

    @Override
    public String toString() {
        return "Employee { id=" + employeeId + ", name=" + employeeName + " }";
    }

    @Override
    public String getBBRowKey() {
        if (getEmployeeId() == null) {
            return null;
        }
        return getEmployeeId().toString();
    }

    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

}
