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
import java.util.Date;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class ClaimCharges implements BlackBoxable {

    /**
     *
     */
    public enum TranType {

        /**
         *
         */
        CREDIT,
        /**
         *
         */
        DEBIT
    }

    private String ccId = null;
    private Date ccDate = null;
    private TranType type = null;
    private Double amount = 0.0;

    /**
     *
     * @param ccId
     * @param amount
     */
    public ClaimCharges(String ccId, Double amount) {
        this.ccId = ccId;
        this.amount = amount;
    }

    /**
     *
     * @return
     */
    public String getCcId() {
        return ccId;
    }

    /**
     *
     * @param ccId
     */
    public void setCcId(String ccId) {
        this.ccId = ccId;
    }

    /**
     *
     * @return
     */
    public Date getCcDate() {
        return ccDate;
    }

    /**
     *
     * @param ccDate
     */
    public void setCcDate(Date ccDate) {
        this.ccDate = ccDate;
    }

    /**
     *
     * @return
     */
    public TranType getType() {
        return type;
    }

    /**
     *
     * @param type
     */
    public void setType(TranType type) {
        this.type = type;
    }

    /**
     *
     * @return
     */
    public Double getAmount() {
        return amount;
    }

    /**
     *
     * @param amount
     */
    public void setAmount(Double amount) {
        this.amount = amount;
    }

    @Override
    public String getBBRowKey() {
        return getCcId();
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
        return "ClaimCharges{" + "ccId=" + ccId + ", ccDate=" + ccDate + ", type=" + type + ", amount=" + amount + '}';
    }

}
