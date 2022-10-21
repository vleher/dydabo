/*
 * Copyright 2017 viswadas leher .
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

import com.dydabo.blackbox.BlackBoxable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** @author leher */
public class Employee extends User implements BlackBoxable {

  private String employeeTitle;
  private Integer salary;

  public Employee() {}

  /** @return */
  public String getEmployeeTitle() {
    return employeeTitle;
  }

  /** @param employeeTitle */
  public void setEmployeeTitle(String employeeTitle) {
    this.employeeTitle = employeeTitle;
  }

  /** @return */
  public Integer getSalary() {
    return salary;
  }

  /** @param salary */
  public void setSalary(Integer salary) {
    this.salary = salary;
  }

  @Override
  public List<Optional<Object>> getBBRowKeys() {
    return Arrays.asList(Optional.ofNullable(getUserId()), Optional.ofNullable("E"));
  }

  @Override
  public String toString() {
    return "Employee{"
        + "employeeTitle='"
        + employeeTitle
        + '\''
        + ", salary="
        + salary
        + "} "
        + super.toString();
  }
}
