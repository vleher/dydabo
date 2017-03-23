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
package com.dydabo.blackbox.beans;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class Customer extends User {

    private Map<String, Address> address = new HashMap<>();
    private List<String> emailAddresses = new ArrayList<>();

    public Customer(Integer userId, String userName) {
        super(userId, userName);
    }

    public void initData() {
        // dummy pre polualted values
        emailAddresses.add(getUserName() + "@dummy.com");
        emailAddresses.add(getUserId() + "@gmail.com");
        emailAddresses.add(getUserName() + "@msn.com");

        address.put("Home", new Address("1234 home", "egan", "mn", "us"));
        address.put("Work", new Address("2377 work st", "minneapolis", "mn", "us"));
        address.put("Other", new Address("999 other", "other", "ca", "us"));
    }

    public Map<String, Address> getAddress() {
        return address;
    }

    public void setAddress(Map<String, Address> address) {
        this.address = address;
    }

    public List<String> getEmailAddresses() {
        return emailAddresses;
    }

    public void setEmailAddresses(List<String> emailAddresses) {
        this.emailAddresses = emailAddresses;
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
        System.out.println("Customer JSON:" + new Gson().toJson(this));
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return "Customer{" + "address=" + address + ", emailAddresses=" + emailAddresses + '}';
    }

}
