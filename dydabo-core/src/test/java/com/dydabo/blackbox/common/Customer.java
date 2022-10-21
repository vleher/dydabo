package com.dydabo.blackbox.common;

import java.util.List;
import java.util.Optional;

import com.dydabo.blackbox.BlackBoxable;

public class Customer implements BlackBoxable {

	private String userName;

	public String getUserName() {
		return userName;
	}

	public void setUserName(final String userName) {
		this.userName = userName;
	}

	@Override
	public List<Optional<Object>> getBBRowKeys() {
		// TODO Auto-generated method stub
		return null;
	}

}
