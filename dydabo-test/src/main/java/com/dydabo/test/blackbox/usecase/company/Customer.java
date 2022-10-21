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

import java.util.*;

/** @author viswadas leher */
public class Customer extends User implements BlackBoxable {

  private Map<String, Address> address = new HashMap<>();
  private List<String> emailAddresses = new ArrayList<>();
  private String[] cars = new String[] {};

  public Customer() {}

  /** */
  public void initData() {
    // dummy pre polualted values
    emailAddresses.add(getUserName() + "@dummy.com");
    emailAddresses.add(getUserId() + "@gmail.com");
    emailAddresses.add(getUserName() + "@msn.com");

    address.put(
        "Home", new Address(new Random().nextInt(10000) + " home ave", "eagan", "mn", "us"));
    address.put(
        "Work", new Address(new Random().nextInt(10000) + " work st", "minneapolis", "mn", "us"));
    address.put(
        "Other", new Address(new Random().nextInt(10000) + " other blvd", "other", "ca", "us"));

    cars = new String[] {"Ford", "Toyota", "BMW"};
  }

  /** @return */
  public Map<String, Address> getAddress() {
    return address;
  }

  /** @param address */
  public void setAddress(Map<String, Address> address) {
    this.address = address;
  }

  /** @return */
  public List<String> getEmailAddresses() {
    return emailAddresses;
  }

  /** @param emailAddresses */
  public void setEmailAddresses(List<String> emailAddresses) {
    this.emailAddresses = emailAddresses;
  }

  /** @return */
  public String[] getCars() {
    return cars;
  }

  /** @param cars */
  public void setCars(String[] cars) {
    this.cars = cars;
  }

  @Override
  public List<Optional<Object>> getBBRowKeys() {
    return Arrays.asList(Optional.ofNullable(getUserId()), Optional.ofNullable("C"));
  }

  @Override
  public String toString() {
    return "Customer{"
        + "address="
        + address
        + ", emailAddresses="
        + emailAddresses
        + ", cars="
        + Arrays.toString(cars)
        + "}"
        + " "
        + super.toString();
  }
}
