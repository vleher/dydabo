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

/**
 * @author viswadas leher
 */
public abstract class User implements BlackBoxable {

	private static final long serialVersionUID = 1L;

	private String userName;
	private String firstName;
	private String lastName;
	private Integer userId;
	private Double taxRate;

	public User() {
	}

	/**
	 * @return
	 */
	public String getUserName() {
		return userName;
	}

	/**
	 * @param userName
	 */
	public void setUserName(final String userName) {
		this.userName = userName;
	}

	/**
	 * @return
	 */
	public Integer getUserId() {
		return userId;
	}

	/**
	 * @param userId
	 */
	public void setUserId(final Integer userId) {
		this.userId = userId;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(final String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(final String lastName) {
		this.lastName = lastName;
	}

	/**
	 * @return
	 */
	public Double getTaxRate() {
		return taxRate;
	}

	/**
	 * @param taxRate
	 */
	public void setTaxRate(final Double taxRate) {
		this.taxRate = taxRate;
	}

	@Override
	public String toString() {
		return "User{" + "userName='" + userName + '\'' + ", firstName='" + firstName + '\'' + ", lastName='" + lastName
				+ '\'' + ", userId=" + userId + ", taxRate=" + taxRate + '}';
	}

}
