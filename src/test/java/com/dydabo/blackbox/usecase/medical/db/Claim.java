/** *****************************************************************************
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************
 */
package com.dydabo.blackbox.usecase.medical.db;

import com.dydabo.blackbox.BlackBoxable;
import com.google.gson.Gson;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class Claim implements BlackBoxable {

    private String cId = null;
    private String pId = null;
    private List<ClaimDetails> cDets = new LinkedList<>();
    private List<ClaimCharges> cCharges = new LinkedList<>();

    /**
     *
     * @param cId
     * @param pId
     */
    public Claim(String cId, String pId) {
        this.cId = cId;
        this.pId = pId;
    }

    /**
     *
     * @return
     */
    public String getcId() {
        return cId;
    }

    /**
     *
     * @param cId
     */
    public void setcId(String cId) {
        this.cId = cId;
    }

    /**
     *
     * @return
     */
    public List<ClaimDetails> getcDets() {
        return cDets;
    }

    /**
     *
     * @param cDets
     */
    public void setcDets(List<ClaimDetails> cDets) {
        this.cDets = cDets;
    }

    /**
     *
     * @return
     */
    public List<ClaimCharges> getcCharges() {
        return cCharges;
    }

    /**
     *
     * @param cCharges
     */
    public void setcCharges(List<ClaimCharges> cCharges) {
        this.cCharges = cCharges;
    }

    @Override
    public String getBBRowKey() {
        return getcId();
    }

    /**
     *
     * @return
     */
    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

    /**
     *
     * @return
     */
    public String getpId() {
        return pId;
    }

    /**
     *
     * @param pId
     */
    public void setpId(String pId) {
        this.pId = pId;
    }

    @Override
    public String toString() {
        return "Claim{" + "cId=" + cId + ", cDets=" + cDets + ", cCharges=" + cCharges + '}';
    }

}
