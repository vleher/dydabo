package com.dydabo.blackbox.beans;

import com.dydabo.blackbox.BlackBoxable;

/**
 * @author viswadas leher
 */
public class SampleBean implements BlackBoxable {

    private final String bbKey;

    public SampleBean(String bbKey) {
        this.bbKey = bbKey;
    }

    @Override
    public String getBBJson() {
        return null;
    }

    @Override
    public String getBBRowKey() {
        return bbKey;
    }
}
