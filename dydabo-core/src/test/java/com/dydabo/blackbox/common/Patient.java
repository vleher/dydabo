package com.dydabo.blackbox.common;

import java.util.List;
import java.util.Optional;

import com.dydabo.blackbox.BlackBoxable;

public class Patient implements BlackBoxable {

	@Override
	public List<Optional<Object>> getBBRowKeys() {
		return null;
	}

}
