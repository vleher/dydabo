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
import java.util.Date;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class ClaimCharges implements BlackBoxable {

    public enum TranType {
        CREDIT, DEBIT
    }

    private String ccId = null;
    private Date ccDate = null;
    private TranType type = null;
    private Double amount = 0.0;

    public ClaimCharges(String ccId, Double amount) {
        this.ccId = ccId;
        this.amount = amount;
    }

    public String getCcId() {
        return ccId;
    }

    public void setCcId(String ccId) {
        this.ccId = ccId;
    }

    public Date getCcDate() {
        return ccDate;
    }

    public void setCcDate(Date ccDate) {
        this.ccDate = ccDate;
    }

    public TranType getType() {
        return type;
    }

    public void setType(TranType type) {
        this.type = type;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    @Override
    public String getBBRowKey() {
        return getCcId();
    }

    @Override
    public String getBBJson() {
        return new Gson().toJson(this);
    }

    @Override
    public String toString() {
        return "ClaimCharges{" + "ccId=" + ccId + ", ccDate=" + ccDate + ", type=" + type + ", amount=" + amount + '}';
    }

}
