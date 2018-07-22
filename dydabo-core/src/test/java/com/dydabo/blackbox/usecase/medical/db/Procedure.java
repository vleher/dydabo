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

/**
 * @author viswadas leher
 */
public class Procedure implements BlackBoxable {

    private String pId = null;
    private String pName = null;
    private String pNotes = null;

    /**
     *
     */
    public Procedure() {
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
    public String getpName() {
        return pName;
    }

    /**
     * @param pName
     */
    public void setpName(String pName) {
        this.pName = pName;
    }

    /**
     * @return
     */
    public String getpNotes() {
        return pNotes;
    }

    /**
     * @param pNotes
     */
    public void setpNotes(String pNotes) {
        this.pNotes = pNotes;
    }

    @Override
    public String getBBRowKey() {
        return getpId();
    }

    /**
     * @return
     */
    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

}
