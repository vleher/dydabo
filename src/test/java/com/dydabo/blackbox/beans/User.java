/*******************************************************************************
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
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
 *******************************************************************************/
package com.dydabo.blackbox.beans;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public abstract class User {

    private String userName;
    private Integer userId;
    private Double taxRate;

    /**
     *
     * @param userId
     * @param userName
     */
    public User(Integer userId, String userName) {
        this.userId = userId;
        this.userName = userName;
    }

    /**
     *
     * @return
     */
    public String getUserName() {
        return userName;
    }

    /**
     *
     * @param userName
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     *
     * @return
     */
    public Integer getUserId() {
        return userId;
    }

    /**
     *
     * @param userId
     */
    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    /**
     *
     * @return
     */
    public Double getTaxRate() {
        return taxRate;
    }

    /**
     *
     * @param taxRate
     */
    public void setTaxRate(Double taxRate) {
        this.taxRate = taxRate;
    }

}
