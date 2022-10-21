package com.dydabo.test.blackbox.beans;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.dydabo.blackbox.BlackBoxable;

/** @author viswadas leher */
public class SampleBean implements BlackBoxable {

	private static final long serialVersionUID = 1L;
	private final String bbKey;

	public SampleBean(final String bbKey) {
		this.bbKey = bbKey;
	}

	@Override
	public List<Optional<Object>> getBBRowKeys() {
		return Arrays.asList(Optional.ofNullable(bbKey));
	}
}
