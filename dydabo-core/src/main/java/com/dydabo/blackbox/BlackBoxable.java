/*
 * Copyright 2020 viswadas leher <vleher@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.dydabo.blackbox;

import com.dydabo.blackbox.common.utils.DyDaBoConstants;
import com.dydabo.blackbox.gson.InstantAdapter;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * The interface that needs to be implemented by the POJO that needs to be saved into the Database.
 *
 * @author viswadas leher
 */
public interface BlackBoxable extends Serializable {
	/**
	 * A Json representation of the POJO
	 *
	 * @return a valid Json string
	 */
	default String getBBJson() {
		GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeAdapter(Instant.class, new InstantAdapter());
		return gsonBuilder.create().toJson(this);
	}

	/**
	 * Return an ordered list of fields that will used as row key, will create a row key that will
	 * be used to store the object to the database
	 *
	 * @return a list of fields used for the row key or an unique identifier
	 */
	List<Optional<Object>> getBBRowKeys();

	default String getBBRowKey() {
		StringJoiner keyBuilder = new StringJoiner(DyDaBoConstants.KEY_SEPARATOR);
		getBBRowKeys().forEach(o -> {
			if (o.isEmpty()) {
				keyBuilder.add(".*");
			} else if (o.get() instanceof Instant) {
				keyBuilder.add(
						String.valueOf(((Instant) o.get()).getLong(ChronoField.INSTANT_SECONDS)));
			} else {
				keyBuilder.add(o.get().toString());
			}
		});

		return keyBuilder.toString();
	}
}
