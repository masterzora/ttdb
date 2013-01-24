ttdb
====

A simple in-memory transactional database with a client/server setup.

The project was prompted by Thumbtack's programming challenge #2 (http://www.thumbtack.com/challenges) and thus works to meet those parameters even while extending functionality.


To get running quickly simply start first the server program (TTDB.py) and then the client (TTDBClient.py) from the same location.  You should have permissions to create and use a Unix socket in that location.

Run each with a -h flag for more advanced usage info, including specifying a different socket location.


Supported client commands:

Writes:
 * SET variable value
  * Sets variable to the given value
 * UNSET variable
  * Deletes the given variable

Reads:
 * GET variable
  * Retrieves the value of the given variable.  Outside a transaction this is the current value in the table.  In a transaction it is either the latest value set by the transaction if it has been or the value in the table as of the beginning of the transaction.
 * NUMEQUALTO value
  * Retrieves the number of variables with the given value. Outside a transaction this is the current count in the table.  In a transaction the count uses the count as of the beginning of the transaction plus or minus any modification within the transaction.


Transactions:
 * BEGIN [RW|RO]
  * Opens a new transaction or nested subtransaction.  A new transaction can be either read-write (RW) or read-only (RO) and defaults to read-write if neither option is given.  A nested subtransaction copies the type of its parent.
 * ROLLBACK
  * Rolls back a transaction without committing, clearing all stored changes.  If transactions are nested this only rolls back one layer keeping the parent transactions open.
 * COMMIT
  * Commits a transaction, saving all changes to the main table.  If transactions are nested this collapses all the way rather than committing only one level.

* END
 * Exits the client program.
