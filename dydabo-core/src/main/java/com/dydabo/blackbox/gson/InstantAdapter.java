/*
 * Copyright 2021 viswadas leher <vleher@gmail.com>.
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

package com.dydabo.blackbox.gson;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.time.Instant;

public class InstantAdapter extends TypeAdapter<Instant> {
	@Override
	public void write(JsonWriter out, Instant value) throws IOException {
		out.beginObject();
		if (value != null) {
			out.name("instant");
			out.value(value.toEpochMilli());
		}
		out.endObject();
	}

	@Override
	public Instant read(JsonReader in) throws IOException {
		in.beginObject();
		Instant ins = null;
		if (in.hasNext()) {
			ins = Instant.ofEpochMilli(in.nextLong());
		}
		in.endObject();
		return ins;
	}
}
