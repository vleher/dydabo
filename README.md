# dydabo
Dynamic Database BlackBox (DyDaBo) was born out of several different Data Abstraction modules that I had written for several different projects.
The idea here is to create an abstraction that will "completely" eliminate the need to know how your data is stored in the backend.

Background
---------------------------------
In a relational database, you need to know what data you are going to store and how you are going to access it, usually ahead of time. Depending on these requirements, you come up with a database schema that is normalized enough to give you a good performance for storing and querying. Everytime you wanted to add a new piece of data, you had to modify the database schema and modify your code to handle the new piece of data.

All that kind of changed with the NoSql databases. Now, you can insert columns on the fly and deal with large amounts of data.

With DyDaBo, the idea is that you can create POJO (Plain Old Java Object) just as you would to represent data in your project. These object will (or can) represent a database table in the backend. The library will take care of creating the database table. It can add new columns to represent the fields in the POJO and store data into these tables.

Basically, you can add and remove fields as you like, which is beneficial during development. These POJO can then be saved onto the database without the user having to implement any backend specific code. You can insert, update, delete and fetch these objects from the database as needed, by using nothing more than the POJO.

Supported Databases
-----------------------------------
Currently, HBase/Hadoop is the database of choice and the only one that is supported. Other NoSql databases are being worked on.


Status of the Project
-----------------------------------
The project is still a work in progress. This was initially meant to be something that would let me learn different NoSql databases, and still remains the prime objective.

There are still several advanced features that needs to be implemented, and the interfaces are bound to change. So, all in all it is probably not good for prime time.

I have used it in a couple of simple projects and it has so far worked quite well.

Usage
-----------------------------------

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Insert, Update, Delete and Search the POJO to the backend

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
BlackBox instanceOne = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);
User user = new User(21, "name");
List<User> userList = new ArrayList<>();
userList.add(new User(123, "David Jones");
userList.add(new User(234, "Lady Gaga");
// insert to database
boolean success = instanceOne.insert(user);
// update the existing row
success = instanceOne.update(user);
// delete the row
success = instanceOne.delete(user);
// search/get for the row
User u = new User(null, "David.*");
// this will search all users with the name David
List<User> searchResults = instanceOne.search(u);

List<String> rowKeys = new ArrayList<>();
rowKeys.add("key-one");
rowKeys.add("key-two");
List<User> rowList = instanceOne.fetch(rowKeys);
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is just a generalized simple example to demonstrate the functionality of the library.

Current Dependencies
----------------------------------------------------------

 * Google Gson 
 * Hbase driver

Current Status
----------------------------------------------------------

Early Alpha


