# dydabo

**Dy**namic **Da**tabase Black**Bo**x (**dydabo**) was born out of several different data abstraction modules that I had
written for several different projects. The idea here is to create an abstraction that will ***completely*** eliminate
the need to know how your data is stored in the back end or data store.

In that sense this can probably be classified as a Object Data Modeling (ODM) library, rather than a Object Mapping
library like Hibernate. Although this library does make some implicit object mapping.

### Background

In the world of relational databases, you need to know what data you are going to store and how you are going to access
it, usually ahead of time. Depending on the requirements, you come up with a database schema that is normalized and
efficient enough to give you a good performance for both storing and querying. Every time you need to add a new piece of
data, you will modify the database schema and modify your code to handle the new piece of data.

All that *kind* of changed with the NoSql databases. Now, you can insert columns (and data) on the fly and deal with
large amounts of data. You still need to know how you are going to access the data, so that you can formulate the data
and row keys appropriately.

### Goals

**dydabo** is a Java library that can be used to convert and persist Plain Old Java Objects (POJO) into one or several
back end databases. The library should take care of almost all database activities, such as the creation of tables,
insertion, update, deletion of row from the tables etc. Also, the library should shield the users completely from any of
the database knowledge or information.

The eventual goal of the library can be summed up as below:

* Create tables in the back end based on POJO, with out the need for any user intervention.
* Ability to store fields and values in the table given a POJO
* Ability to query and retrieve data from the back end based on a POJO
* Ability to add new fields to POJO or columns to the database easily
* No need to learn a query language to access data (SQL, CQL, HQL etc. etc.)
* Ability to use regular expressions or wildcard as a query language to match against values
* No configuration or XML files to maintain in the source
* No (or very minimal) use of annotations in the source code
* Ability to store data in to multiple databases (such as Apache HBase, Apache Cassandra, CouchDB, MongoDB etc)
* Good performance for most and common search use cases

### Audience

**So, who can use this?** Anybody who can set it up and get it running can use it....and good luck with that. ;-)

**Who should use this?** Just some of different scenarios that I can think of:

* Someone who wants to develop an application quickly and easily without worrying about the back end. You can use this
  during the development phase of the project, before you have a complete understanding of all your requirements and
  query/access details. Once you have completed the application, you can identify the types of data access you need, and
  you will/may be able to come up with a better database design.
* You don't necessarily need to have terabytes of data to use a NoSql database. Any small or medium size application can
  use it just as effectively. A simple application that do not have tons of data, and where every query need not be
  tuned to the fraction of a milli-second may find this easier to maintain.
* You don't want to dabble with SQL, CQL, HQL queries. If you design your POJO ***wisely*** so as to easily access most
  of the data you want, with good row key selection then the library should suffice.

**Who should not use this?**

* Developers or Users who love to configure everything using XML files and/or annotations.
* Companies with lots of resources and tons of data. You should ideally be writing customized libraries that works best
  for your problem/use case.

There will be several limitations to the library when compared to the vanilla database driver functionality. Refer to
the limitations section below.

### Limitations

* It is very unlikely that every single functionality and flexibility of query execution that the database driver
  supports will be exposed through this library. If you need such granularity, flexibility and power then you will be
  better off using the driver directly.
* If you want ultimate control over how your data is stored and managed then this is **NOT** the library for you. The
  idea here is to completely isolate the user/developer from *low level* database design and management.
* Dependency on Google Gson library and HBase java client libraries. And of course there is dependency on an underlying driver for each of the database. I have tried to stick with the most "official" version of the driver wherever possible.

### Supported Databases

Support for the following databases:

* Apache HBase/Hadoop
* Apache Cassandra
* MongoDB
* Redis

### Status of the Project

The project is still a work in progress with functionality build as needed. I have been using it many and all of my projects for many years now.

There are still several features that needs to be implemented, and the interfaces are bound to change. As I am the only one using it, I have not paid much attention to keeping the API stable.

### Documentation and Use Cases

* dydabo API: Coming soon.
* dydabo [User Guide](https://github.com/vleher/dydabo/blob/master/USERGUIDE.md): This guide contains examples and some
  use cases on how to use dydabo in your project and code.

### Build Status

<img src="https://circleci.com/gh/vleher/dydabo.png?style=shield&circle-token=:circle-token">

### License

dydabo is released under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0  "Apache 2.0 License")

````
 Copyright 2023 viswadas leher .

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
