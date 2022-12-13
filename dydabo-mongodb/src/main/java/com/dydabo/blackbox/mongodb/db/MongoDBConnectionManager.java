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
package com.dydabo.blackbox.mongodb.db;

import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

/**
 * @author viswadas leher
 */
public class MongoDBConnectionManager {

	private static final Object lockObj = new Object();
	private static MongoClient mongoClient = null;

	private final String connectionString;
	private final String database;
	private final String collection;

	public MongoDBConnectionManager(String connectionString, String database, String collection) {
		this.connectionString = connectionString;
		this.database = database;
		this.collection = collection;
	}

	public MongoCollection<Document> getMongoDBCollection() {
		return getMongoDBCollection(connectionString, database, collection);
	}

	/**
	 * @param connString
	 * @param database
	 * @param collection
	 * @return
	 */
	private MongoCollection<Document> getMongoDBCollection(String connString, String database,
			String collection) {
		if (mongoClient == null) {
			if (DyDaBoUtils.isBlankOrNull(connString)) {
				connString = "mongodb://localhost:27017";
			}
			synchronized (lockObj) {
				mongoClient = new MongoClient(new MongoClientURI(connString));
			}
		}
		return mongoClient.getDatabase(database).getCollection(collection);
	}
}
