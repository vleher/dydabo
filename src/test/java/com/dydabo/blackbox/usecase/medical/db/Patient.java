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
package com.dydabo.blackbox.usecase.medical.db;

import com.dydabo.blackbox.BlackBoxable;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class Patient implements BlackBoxable {

    private String pId = null;
    private String fN = null;
    private String lN = null;
    private Date dob = null;
    private Map<String, Address> address = new HashMap<>();
    private List<String> em = new ArrayList<>();
    private PhoneNumber pPh = null;

    public Patient() {
    }

    public Patient(String id, String firstName, String lastName) {
        this.pId = id;
        this.fN = firstName;
        this.lN = lastName;
    }

    public void initData() {
        // dummy pre polualted values
        em.add(getpId() + "@hospital.com");
        em.add(getfN() + "." + getlN() + "@gmail.com");
        em.add(getpPh() + "@msn.com");

        address.put("Home", new Address(new Random().nextInt(10000) + " home ave", "egan", "mn", "us"));
        address.put("Work", new Address(new Random().nextInt(10000) + " work st", "minneapolis", "mn", "us"));
        address.put("Other", new Address(new Random().nextInt(10000) + " other blvd", "other", "ca", "us"));

        Random rnd = new Random();
        long now = new Date().getTime();
        long ms = rnd.nextLong();

        while (ms > now) {
            ms = ms / 2;
        }
        setDob(new Date(ms));
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public String getfN() {
        return fN;
    }

    public void setfN(String fN) {
        this.fN = fN;
    }

    public String getlN() {
        return lN;
    }

    public void setlN(String lN) {
        this.lN = lN;
    }

    public Date getDob() {
        return dob;
    }

    public void setDob(Date dob) {
        this.dob = dob;
    }

    public Map<String, Address> getAddress() {
        return address;
    }

    public void setAddress(Map<String, Address> address) {
        this.address = address;
    }

    public PhoneNumber getpPh() {
        return pPh;
    }

    public void setpPh(PhoneNumber pPh) {
        this.pPh = pPh;
    }

    @Override
    public String getBBRowKey() {
        return getpId() + ":" + getfN() + ":" + getlN();
    }

    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return "Patient{" + "pId=" + pId + ", fN=" + fN + ", lN=" + lN + ", dob=" + dob + ", address=" + address + ", pPh=" + pPh + '}';
    }

}
