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
package com.dydabo.blackbox.mongodb.tasks;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.mongodb.utils.MongoUtils;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.concurrent.RecursiveTask;

public abstract class MongoBaseTask<T extends BlackBoxable, R> extends RecursiveTask<R> {
	private final MongoCollection<Document> collection;
	private final MongoUtils<T> utils;

	public MongoBaseTask(MongoCollection<Document> collection) {
		this.collection = collection;
		this.utils = new MongoUtils<>();
	}

	public MongoCollection<Document> getCollection() {
		return collection;
	}

	public MongoUtils<T> getUtils() {
		return utils;
	}
}
