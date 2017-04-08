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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class Customer extends User implements BlackBoxable {

    private Map<String, Address> address = new HashMap<>();
    private List<String> emailAddresses = new ArrayList<>();
    private String[] cars = new String[]{};

    /**
     *
     * @param userId
     * @param userName
     */
    public Customer(Integer userId, String userName) {
        super(userId, userName);
    }

    /**
     *
     */
    public void initData() {
        // dummy pre polualted values
        emailAddresses.add(getUserName() + "@dummy.com");
        emailAddresses.add(getUserId() + "@gmail.com");
        emailAddresses.add(getUserName() + "@msn.com");

        address.put("Home", new Address(new Random().nextInt(10000) + " home ave", "eagan", "mn", "us"));
        address.put("Work", new Address(new Random().nextInt(10000) + " work st", "minneapolis", "mn", "us"));
        address.put("Other", new Address(new Random().nextInt(10000) + " other blvd", "other", "ca", "us"));

        cars = new String[]{"Ford", "Toyota", "BMW"};
    }

    /**
     *
     * @return
     */
    public Map<String, Address> getAddress() {
        return address;
    }

    /**
     *
     * @param address
     */
    public void setAddress(Map<String, Address> address) {
        this.address = address;
    }

    /**
     *
     * @return
     */
    public List<String> getEmailAddresses() {
        return emailAddresses;
    }

    /**
     *
     * @param emailAddresses
     */
    public void setEmailAddresses(List<String> emailAddresses) {
        this.emailAddresses = emailAddresses;
    }

    /**
     *
     * @return
     */
    public String[] getCars() {
        return cars;
    }

    /**
     *
     * @param cars
     */
    public void setCars(String[] cars) {
        this.cars = cars;
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
        return "Customer{ userID=" + getUserId() + ", UserName=" + getUserName() + ", address=" + address +
                ", emailAddresses=" + emailAddresses + '}' + super.toString();
    }

}
