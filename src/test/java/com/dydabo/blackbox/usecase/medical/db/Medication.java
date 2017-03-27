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

    /**
     *
     */
    public Medication() {
    }

    /**
     *
     * @param mId
     */
    public Medication(String mId) {
        this.mId = mId;
        this.mName = mId;
    }

    /**
     *
     * @return
     */
    public String getmId() {
        return mId;
    }

    /**
     *
     * @param mId
     */
    public void setmId(String mId) {
        this.mId = mId;
    }

    /**
     *
     * @return
     */
    public String getmName() {
        return mName;
    }

    /**
     *
     * @param mName
     */
    public void setmName(String mName) {
        this.mName = mName;
    }

    /**
     *
     * @return
     */
    public Integer getmDose() {
        return mDose;
    }

    /**
     *
     * @param mDose
     */
    public void setmDose(Integer mDose) {
        this.mDose = mDose;
    }

    /**
     *
     * @return
     */
    public String getmVendor() {
        return mVendor;
    }

    /**
     *
     * @param mVendor
     */
    public void setmVendor(String mVendor) {
        this.mVendor = mVendor;
    }

    @Override
    public String getBBRowKey() {
        return getmName();
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
        return "Medication{" + "mId=" + mId + ", mName=" + mName + ", mDose=" + mDose + ", mVendor=" + mVendor + '}';
    }

}
