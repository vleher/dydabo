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
package com.dydabo.test.blackbox.usecase.medical.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import com.dydabo.blackbox.BlackBoxable;

/**
 * @author viswadas leher
 */
public class Patient implements BlackBoxable {

	private static final long serialVersionUID = 1L;

	private String patientId = null;
	private String firstName = null;
	private String lastName = null;
	private Date dateOfBirth = null;
	private Map<String, Address> address = new HashMap<>();
	private final List<String> emailAddress = new ArrayList<>();
	private PhoneNumber phoneNumber = null;

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
	public Patient(final String id, final String firstName, final String lastName) {
		this.patientId = id;
		this.firstName = firstName;
		this.lastName = lastName;
	}

	/**
	 *
	 */
	public void initData() {
		// dummy pre populated values
		emailAddress.add(getPatientId() + "@hospital.com");
		emailAddress.add(getFirstName() + "." + getLastName() + "@gmail.com");
		emailAddress.add(getPhoneNumber() + "@msn.com");

		address.put("Home", new Address(new Random().nextInt(10000) + " home ave", "egan", "mn", "us"));
		address.put("Work", new Address(new Random().nextInt(10000) + " work st", "minneapolis", "mn", "us"));
		address.put("Other", new Address(new Random().nextInt(10000) + " other blvd", "other", "ca", "us"));

		final Random rnd = new Random();
		final long now = new Date().getTime();
		long ms = rnd.nextLong();

		while (ms > now) {
			ms = ms / 2;
		}
		setDateOfBirth(new Date(ms));
	}

	/**
	 * @return
	 */
	public String getPatientId() {
		return patientId;
	}

	/**
	 * @param patientId
	 */
	public void setPatientId(final String patientId) {
		this.patientId = patientId;
	}

	/**
	 * @return
	 */
	public String getFirstName() {
		return firstName;
	}

	/**
	 * @param firstName
	 */
	public void setFirstName(final String firstName) {
		this.firstName = firstName;
	}

	/**
	 * @return
	 */
	public String getLastName() {
		return lastName;
	}

	/**
	 * @param lastName
	 */
	public void setLastName(final String lastName) {
		this.lastName = lastName;
	}

	/**
	 * @return
	 */
	public Date getDateOfBirth() {
		return dateOfBirth;
	}

	/**
	 * @param dateOfBirth
	 */
	public void setDateOfBirth(final Date dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
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
	public void setAddress(final Map<String, Address> address) {
		this.address = address;
	}

	/**
	 * @return
	 */
	public PhoneNumber getPhoneNumber() {
		return phoneNumber;
	}

	/**
	 * @param phoneNumber
	 */
	public void setPhoneNumber(final PhoneNumber phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	@Override
	public List<Optional<Object>> getBBRowKeys() {
		return Arrays.asList(Optional.ofNullable(getPatientId()), Optional.ofNullable(getFirstName()),
				Optional.ofNullable(getLastName()));
	}

	@Override
	public String toString() {
		return "Patient{" + "pId=" + patientId + ", fN=" + firstName + ", lN=" + lastName + ", dob=" + dateOfBirth
				+ ", address=" + address + ", pPh=" + phoneNumber + '}';
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof Patient) {
			final Patient p = (Patient) obj;
			return getPatientId().equals(p.getPatientId()) && getFirstName().equals(p.getFirstName())
					&& getLastName().equals(p.getLastName());
		}
		return false;
	}
}
