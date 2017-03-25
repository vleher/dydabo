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

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class Medication implements BlackBoxable {

    private String mId = null;
    private String mName = null;
    private Integer mDose = null;
    private String mVendor = null;

    public Medication() {
    }

    public Medication(String mId) {
        this.mId = mId;
        this.mName = mId;
    }

    public String getmId() {
        return mId;
    }

    public void setmId(String mId) {
        this.mId = mId;
    }

    public String getmName() {
        return mName;
    }

    public void setmName(String mName) {
        this.mName = mName;
    }

    public Integer getmDose() {
        return mDose;
    }

    public void setmDose(Integer mDose) {
        this.mDose = mDose;
    }

    public String getmVendor() {
        return mVendor;
    }

    public void setmVendor(String mVendor) {
        this.mVendor = mVendor;
    }

    @Override
    public String getBBRowKey() {
        return getmName();
    }

    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return "Medication{" + "mId=" + mId + ", mName=" + mName + ", mDose=" + mDose + ", mVendor=" + mVendor + '}';
    }

}
