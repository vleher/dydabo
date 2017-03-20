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
package com.dydabo.blackbox.test.beans;

import com.dydabo.blackbox.BlackBoxable;
import com.google.gson.Gson;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class User implements BlackBoxable {

    private String userName;
    private Integer userId;

    public User() {
    }

    public User(Integer userId, String userName) {
        this.userId = userId;
        this.userName = userName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    @Override
    public String getBBRowKey() {
        if (getUserId() == null) {
            return null;
        }
        return getUserId().toString();
    }

    @Override
    public String toString() {
        return "User{" + "userName=" + userName + ", userId=" + userId + '}';
    }

    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

}
