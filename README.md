# dydabo
Dynamic Database BlackBox (DyDaBo) was born out of several different Data Abstraction modules that I had written for several different projects.
The idea here is to create an abstraction that will "completely" eliminate the need to know how your data stored in the backend.

Background
---------------------------------
In the context of a relational database, you need to know what data you are going to store and how you are going to access it. Depending on these
requirements, you come up with a database scheme that is normalized enough to give you a good performance. Everytime you wanted to add a new
piece of data, you had to modify the database schema and modify your code to handle the new piece of data. All that changed with the
NoSql databases. Now, you can insert columns on the fly.

With DyDaBo you can create POJO (Plain Old Java Object) to represent a database table. You can add and remove fields as you like, which is
beneficial during development. You can insert, update, delete and fetch these objects from the database as needed.

Supported Databases
-----------------------------------
Currently, HBase is the database of choice and the only one that is supported.


Status
-----------------------------------
The project is still a work in progress. This was initially meant to be something that would let me learn different NoSql databases, and still
remains to be the prime objective.

There are still several advanced features that needs to be implemented, and the interfaces are bound to change.

I have used it in a couple of simple projects and it has worked quite well.


