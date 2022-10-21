# dydabo User Guide

### Table of Contents

1. [Overview](#overview)
2. [Using dydabo](#usingdydabo)
3. [Use Cases](#usecases)

### Overview <a name="overview"></a>

**dydabo** is a Java library that can be used to convert and persist Plain Old Java Objects (POJO) into one or several
back end databases. It is a data modeling and abstraction tool that allows for easy storage and access of data from a
back end database.

### (Current) Limitations

* It is very unlikely that every single functionality and flexibility of query execution that the database driver
  supports will be exposed through this library. If you need such granularity, flexibility and power then you will be
  better off using the driver directly.
* If you want ultimate control over how your data is stored and managed then this is **NOT** the library for you. The
  idea here is to completely isolate the user/developer from "*low level*" database design and management.
* As of now, the class name is mapped to the table name. This means if you change the name of your POJO, it will create
  a new table. Your old table won't be deleted but you cannot access that data anymore using the new POJO. Using
  annotation for mapping could be implemented at some time in the future to allow for some flexibility.
* The variable names are mapped to column names (and sometimes family names) which means you will need to do some extra
  work to still access the old column names if you change your variable name after going to production.
* Generic search is implemented using regular expressions. That means Regular Expression filter is used (at this time)
  to match the rowsToDelete in many generic searches, which can be quite slow in case of large data sets.

### Using dydabo <a name="usingdydabo"></a>

The primary class to use with dydabo is **BlackBox** which you can get by calling one of the factory methods in **
BlackBoxFactory**. The **BlackBox** is an interface and the underlying implementation does not maintain any state. That
means that you can use the instance for any number of operations with multiple different objects.

The **BlackBox** instance can be accessed using either of the following statements (for HBase):

`````
BlackBox instanceOne = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);
BlackBox instanceTwo = BlackBoxFactory.getHBaseDatabase(config);
`````

Create a POJO or your data beans in your project. You could think of each of the POJO as a database table. That means
you can add all the data that you want to easily access in to a single class.

Consider a simple class **User** that contains several user specific information such *user name*, *user id*, *first
name*, *last name* etc.

```
public class User implements BlackBoxable {

	private String userName;
	private Integer userId;
	private String firstName;
	private String lastName;

	public User(Integer userId, String userName) {
		this.userId = userId;
		this.userName = userName;
	}

   // other getters and setters go here
    ......

}
```

Every POJO that you want to store in the back end will have to implement the interface **BlackBoxable**. The **
BlackBoxable** interface contains two methods *getBBJson* and *getBBRowKey*.

The ***getBBJson*** method should return a string in valid JSON format, which is a representation of the current
instance of the POJO.

The ***getBBRowKey*** method should return a string that can be used as the key to this object instance. This will be
the row key to the underlying database row, and can be used to fetch the row back from the database.

Let's see how the above **User** class could implement these methods.

```
@Override
public String getBBJson() {
    // in most cases, such an implementation should work.
	return new Gson().toJson(this);
}

@Override
public String getBBRowKey() {
	// a delimiter separated key for easy access using id and/or name
	return getUserId() + ":" + getUserName();
}
```

Now you can insert, update, delete, fetch and search the data using the POJO. Some of the ways such common usages could
be implemented:

```
// Get the database instance 
BlackBox instanceOne = BlackBoxFactory.getDatabase(BlackBoxFactory.HBASE);

// Create a POJO instance
User user = new User(21, "name");
// insert the POJO into the table as a row
instanceOne.insert(user);

// Create several different instances of User and add to a list
List<User> userList = new ArrayList<>();
userList.add(new User(123, "David Jones");
userList.add(new User(234, "Tom Hardy");
// insert to database
boolean success = instanceOne.insert(userList);

// update the existing row
success = instanceOne.update(user);

// delete the row
success = instanceOne.delete(user);

// search/get for the rowsToDelete where user name starts with "David" 
User u = new User(null, "^David.*");
// this will return all users with the name starting with David
List<User> searchResults = instanceOne.search(u);

// As we have used the user name in the row key, we can also get the same information
// by querying the row key, which will be a bit faster than searching.
List<User> allDavids = instanceOne.fetchByPartialKey(".*:David", new User());

// If we know the userId and the username of the user, then you can use the row key 
// to fetch the row.
List<User> currentUser = instanceOne.fetch("1234:David", new User());

// get rowsToDelete using row keys
List<String> rowKeys = new ArrayList<>();
rowKeys.add("1234:David");
rowKeys.add("5321:Tom");
// this will return the users with the specified row keys
List<User> rowList = instanceOne.fetch(rowKeys);
```

> **TIP**: Use fetch(...) if you know the exact row key, which will always be the fastest. Use fetchByPartialKey(...) if you know only part of the row key. Use search(...) if you don't know the key and want to do a column value match.

> **TIP**: Do as many fetch(...) calls as you can, rather than search(...) calls.

> **TIP**: Another way to look at it is ...: design your row keys such that you are much more likely to have the complete row keys in most scenarios rather than not.

### Use Cases <a name="usecases"></a>

It would be beneficial to look at some detailed use cases where you could use this, and how you could do so. I have come
up with some use cases that you might find in a real world application and how best to use this library in such cases.

1. [Hospital Use Case](https://github.com/vleher/dydabo/blob/master/HOSPITALUSECASE.md): A simple example of how
   patients, doctor visits, medications and claims can be designed.

I will try to add more use cases over time. 



