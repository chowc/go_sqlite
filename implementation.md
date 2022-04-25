## Pager

The pager is the only module that accesses (through your operating system's native IO APIs)
native database and journal files. But, it neither interprets the content of databases, nor
modifies the content on its own. (The pager may modify some information in the file header
record, such as the file change counter.) It takes the usual random access/byte-oriented
filesystem operations, and abstracts them into a random access page-based system for
working with database files. It defines an easy-to-use, filesystem-independent interface for
accessing pages from database files. The B+-tree module always uses the pager interface to
access databases, and never directly accesses any database or journal file. It sees the
database file as a logical array of (uniform size) pages.

### Page Cache

- same interface either using in-memory database or persistent database

