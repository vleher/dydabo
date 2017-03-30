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

    /**
     *
     * @param id
     * @param name
     */
    public Employee(Integer id, String name) {
        super(id, name);
    }

    /**
     *
     * @return
     */
    public String getEmployeeTitle() {
        return employeeTitle;
    }

    /**
     *
     * @param employeeTitle
     */
    public void setEmployeeTitle(String employeeTitle) {
        this.employeeTitle = employeeTitle;
    }

    /**
     *
     * @return
     */
    public Integer getSalary() {
        return salary;
    }

    /**
     *
     * @param salary
     */
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

    /**
     *
     * @return
     */
    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return "Employee{ userID=" + getUserId() + ", UserName=" + getUserName() + ", employeeTitle=" + employeeTitle +
                ", salary=" + salary + '}' + super.toString();
    }

}
