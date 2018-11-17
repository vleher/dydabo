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
package com.dydabo.blackbox.usecase.medical.db;

import com.dydabo.blackbox.BlackBoxable;
import com.google.gson.Gson;

import java.util.*;

/**
 * @author viswadas leher
 */
public class Patient implements BlackBoxable {

    private String pId = null;
    private String fN = null;
    private String lN = null;
    private Date dob = null;
    private Map<String, Address> address = new HashMap<>();
    private List<String> em = new ArrayList<>();
    private PhoneNumber pPh = null;

    /**
     *
     */
    public Patient() {
    }

    /**
     * @param id
     * @param firstName
     * @param lastName
     */
    public Patient(String id, String firstName, String lastName) {
        this.pId = id;
        this.fN = firstName;
        this.lN = lastName;
    }

    /**
     *
     */
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

    /**
     * @return
     */
    public String getpId() {
        return pId;
    }

    /**
     * @param pId
     */
    public void setpId(String pId) {
        this.pId = pId;
    }

    /**
     * @return
     */
    public String getfN() {
        return fN;
    }

    /**
     * @param fN
     */
    public void setfN(String fN) {
        this.fN = fN;
    }

    /**
     * @return
     */
    public String getlN() {
        return lN;
    }

    /**
     * @param lN
     */
    public void setlN(String lN) {
        this.lN = lN;
    }

    /**
     * @return
     */
    public Date getDob() {
        return dob;
    }

    /**
     * @param dob
     */
    public void setDob(Date dob) {
        this.dob = dob;
    }

    /**
     * @return
     */
    public Map<String, Address> getAddress() {
        return address;
    }

    /**
     * @param address
     */
    public void setAddress(Map<String, Address> address) {
        this.address = address;
    }

    /**
     * @return
     */
    public PhoneNumber getpPh() {
        return pPh;
    }

    /**
     * @param pPh
     */
    public void setpPh(PhoneNumber pPh) {
        this.pPh = pPh;
    }

    @Override
    public String getBBRowKey() {
        return getpId() + ":" + getfN() + ":" + getlN();
    }

    /**
     * @return
     */
    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return "Patient{" + "pId=" + pId + ", fN=" + fN + ", lN=" + lN + ", dob=" + dob + ", address=" + address + ", pPh=" + pPh + '}';
    }

}
