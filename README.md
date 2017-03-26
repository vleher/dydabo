# dydabo
**Dy**namic **Da**tabase Black**Bo**x (**dydabo**) was born out of several different data abstraction modules that I had written for several different projects.
The idea here is to create an abstraction that will "*completely*" eliminate the need to know how your data is stored in the back end or data store.

In that sense this can probably be classified as a Object Data Modeling (ODM) library, instead of a Object Mapping library like Hibernate.

### Background

In the world of relational databases, you need to know what data you are going to store and how you are going to access it, usually ahead of time. Depending on these requirements, you come up with a database schema that is normalized and efficient enough to give you a good performance for both storing and querying. Every time you wanted to add a new piece of data, you had to modify the database schema and modify your code to handle the new piece of data.

All that *kind* of changed with the NoSql databases. Now, you can insert columns (and data) on the fly and deal with large amounts of data. You still need to know how you are going to access the data, so that you can formulate the data and row keys appropriately.

### Goals

**dydabo** is a Java library that can be used to convert and persist Plain Old Java Objects (POJO) into one or several back end databases. The library should take care of almost all database activities, such as the creation of tables, insertion, update, deletion of row from the tables etc. Also, the library should shield the users from learning yet another query language to get data out of the data store.

The eventual goal of the library can be summed up as below:

* Create tables in the back end based on POJO, with out the need for any user intervention.
* Ability to store fields and values in the table given a POJO
* Ability to query and retrieve data from the back end based on a POJO
* Ability to add new fields to POJO or columns to the database easily
* No need to learn a query language to access data (SQL, CQL, HQL etc. etc.)
* Ability to use regular expressions as query language to match against values
* No configuration or XML files to maintain with class names and variables
* No to minimal use of annotations required
* Ability to store data in to multiple databases (such as Apache HBase, Apache Cassandra, CouchDB, MongoDB etc)
* Good performance for most and common search use cases


### Audience

**So, who can use this?** Anybody who can get it up and running can use it. 

**Who should use this?** Just some of different scenarios that I can think of:

* Someone who wants to develop an application quickly and easily without worrying about the back end. You can use this during the development phase of the project, before you have a complete understanding of all your requirements and query/access details. Once you have completed the application, you can identify the types of data access you need, and you will/may be able to come up with a better database design.
* You don't necessarily need to have terabytes of data to use a NoSql database. Any small or medium size application can use it just as effectively. A simple application that do not have tons of data, and every query need not be tuned to the fraction of a milli-second may find this easier to maintain.
* You don't want to dabble with SQL, CQL, HQL queries. If you design your POJO "*wisely*" so as to easily access most of the data you want, with good row key selection then the library should suffice. 

**Who should not use this?** There will be several limitations to the library when compared to the vanilla database driver functionality. Refer to the limitations section below.


### Supported Databases

Currently, HBase (and Hadoop) is the database of choice and the only one that is supported. Other NoSql databases are being worked on.

### Limitations

* It is very unlikely that every single functionality and flexibility of query execution that the database driver supports will be exposed through this library. If you need such granularity, flexibility and power then you will be better off using the driver directly.
* If you want ultimate control over how your data is stored and managed then this is **NOT** the library for you. The idea here is to completely isolate the developer from "*low level*" database design and management.
* Dependency on Google Gson library and HBase java client libraries.


### Status of the Project

The project is still a work in progress and is in early alpha...so don't expect too much. 

There are still several features that needs to be implemented, and the interfaces are bound to change. So, all in all it is probably not good for prime time. I have used it in a couple of very simple projects and it has so far worked quite well.

### Quick Usage Guide


Get the BlackBox instance, using either of the following statements (for HBase):

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
BlackBox instanceOne = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);
BlackBox instanceTwo = BlackBoxFactory.getHBaseDatabase(config);
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a POJO or your data classes

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
public class User implements BlackBoxable {

	private String userName;
	private Integer userId;
	private Double taxRate;

	public User(Integer userId, String userName) {
		this.userId = userId;
		this.userName = userName;
	}

......

}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now you can insert, update, delete, fetch and search the data using the POJO

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Get the database instance 
BlackBox instanceOne = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);

// Create a POJO instance
User user = new User(21, "name");
// insert the POJO into the table as a row
instanceOne.insert(user);

// Create several different instances of User and add to a list
List<User> userList = new ArrayList<>();
userList.add(new User(123, "David Jones");
userList.add(new User(234, "Lady Gaga");
// insert to database
boolean success = instanceOne.insert(user);

// update the existing row
success = instanceOne.update(user);

// delete the row
success = instanceOne.delete(user);

// search/get for the rows where user name starts with "David" 
User u = new User(null, "David.*");
// this will return all users with the name David
List<User> searchResults = instanceOne.search(u);

// get rows using row keys
List<String> rowKeys = new ArrayList<>();
rowKeys.add("key-one");
rowKeys.add("key-two");
// this will return the users with the specified row keys
List<User> rowList = instanceOne.fetch(rowKeys);
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is just a generalized simple example to demonstrate the functionality of the library.

### Documentation and Use Cases

Coming soon....

### Current Dependencies


 * Google Gson 
 * Hbase java driver
 
### License

dydabo is released under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0  "Apache 2.0 License") 

````
 Copyright 2017 viswadas leher <vleher@gmail.com>.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

````







