#!/usr/bin/python2

import argparse
import datetime
import os
import select
import socket
import sys

class TTDBTable(object):
  """A table and corresponding index for the TT database.

  Attributes:
    table: A dictionary of the database table.
      Format: {key: [[(value, write_stamp), ...], read_stamp], ...}
    index: An dictionary representing the index of the database table values
      Format: {key: [[(value, write_stamp), ...], read_stamp], ...}
    parent: A TTDBTable belonging to the parent transaction
    purge_stamp: A datetime stamp indicating when the last purge was run
    purge_period: An integer indicating the minimum increment between subsequent purges
    autopurge: A boolean representing whether to automatically purge entries on insert
  """
  def __init__(self, parent=None, autopurge=None, purge_period=20):
    """Init TTDBTable with blank table and index.

    Args:
      parent: (keyword) A TTDBTable belonging to the parent transaction
      autopurge: (keyword) A boolean representing whether to automatically purge entries on insert (by default this is set to true if and only if a parent is passed)
      purge_period: (keyword) An integer indicating the minimum increment between subsequent purges
    """
    self.table = {}
    self.index = {}
    self.parent = parent
    self.purge_stamp = datetime.datetime.now()
    self.purge_period = purge_period
    if autopurge is None:
      self.autopurge = parent is not None
    else:
      self.autopurge = autopurge

  def __insert(self, dictionary, key, item):
    """Insert an item, time sorted, to the indicated bucket.

    If key does not exist in dictionary create a new bucket of the format
      [[list of items], read timestamp]
    Else insert the item into the list of items and update the read_timestamp

    Args:
      dictionary: The dictionary into which to insert item
      key: The key where to insert item
      item: Item to be inserted
        Format: (value, timestamp)
    """
    if key not in dictionary:
      dictionary[key] = [[item], item[1]]
    else:
      items = dictionary[key][0]
      for i in range(len(items)):
        if items[i][1] > item[1]:
          break
        else:
          i += 1
      if self.autopurge:
        dictionary[key] = [[item] + items[i:], item[1]]
      else:
        dictionary[key] = [items[:i] + [item] + items[i:], item[1]]

  def __read_item(self, dictionary, key, break_time):
    """Read item from dictionary as it existed at the given time.

    If key is in dictionary and the list of items includes at least one item
    with a write stamp earlier than break_time the latest item earlier than
    break_time is returned and the read timestamp is updated.
    Else, None is returned.

    Args:
      dictionary: The dictionary from which to read item
      key: The key where to read item
      break_time: datetime stamp indicating which snapshot to read

    Returns:
      A tuple of the matching value and its accompanying write timestamp.
    """
    if key not in dictionary:
      return None

    items = dictionary[key][0]
    out_item = None
    for item in items:
      if item[1] > break_time:
        break
      out_item = item
    dictionary[key][1] = break_time
    return out_item

  def __update(self, table, index):
    """Update self's table and index with the values in the passed table and index.

    If the read stamp on self's copy of any key from the passed table or index
    is later than the write stamp of the passed copy the update is aborted.
    Else, all of the values from the passed table and index are inserted into
    self's table and index at the matching keys.

    Args:
      table: The dictionary with which to update self.table
      index: The dictionary with which to update self.index

    Returns:
      A boolean indicating whether the update operation succeeded
    """
    for k,v in table.items():
      if k in self.table and self.table[k][1] > v[1]:
        return False

    for k,v in index.items():
      if k in self.index and self.index[k][1] > v[1]:
        return False

    for k,v in table.items():
      for value in v[0]:
        self.__insert(self.table, k, value)

    for k,v in index.items():
      for value in v[0]:
        self.__insert(self.index, k, value)

    return True

  def read_value(self, key, time):
    """Read item from database as it existed at the given time.

    Args:
      key: The key to read
      time: datetime stamp indicating which snapshot to read

    Returns:
      The matching item from the given time
    """
    return_pair = self.__read_item(self.table, key, time)

    if return_pair is None:
      if self.parent is None:
        return None
      else:
        return self.parent.read_value(key, time)
    else:
      return return_pair[0]

  def read_index(self, value, time):
    """Read item from index as it existed at the given time.

    Args:
      key: The key to read
      time: datetime stamp indicating which snapshot to read

    Returns:
      The matching count of matching values from the given time
    """
    return_pair = self.__read_item(self.index, value, time)

    if return_pair is None:
      if self.parent is None:
        return 0
      else:
        return self.parent.read_index(value, time)
    else:
      return return_pair[0]

  def write_value(self, key, value, time):
    """Write value to the database with the given timestamp

    Insert the new value into key's bucket and update the relevant indexes
    (decrement the old value, if relevant, and increment the new value, if
    relevant).

    Args:
      key: The key to write to
      value: The value to write
      time: datetime stamp to write
    """
    old_value = self.read_value(key, time)
    self.__insert(self.table, key, (value, time))
    if old_value is not None and old_value != value:
      old_index = self.read_index(old_value, time)
      self.__insert(self.index, old_value, (old_index - 1, time))
    if value is not None and old_value != value:
      old_index = self.read_index(value, time)
      self.__insert(self.index, value, (old_index + 1, time))

  def commit(self):
    """Commit the database to the parent database

    Returns:
      A boolean indicating whether the commit succeeded.
    """
    if self.parent is not None:
      return self.parent.__update(self.table, self.index)
    else:
      return False

  def purge_entries(self, time):
    """Purge entries from the database older than the indicated time

    Args:
      time: A datetime stamp indicating the latest time to keep
    """
    if (datetime.datetime.now() - self.purge_stamp).total_seconds() < self.purge_period:
      return

    for key in self.table.keys():
      if len(self.table[key][0]) == 1 and self.table[key][0][0][0] is not None:
        continue
      elif len(self.table[key][0]) == 1 and self.table[key][0][0][0] is None:
        del self.table[key]
      else:
        values = self.table[key][0]
	out_values = []
	for value in values:
          if value[1] > time:
            out_values.append(value)
	if len(out_values) > 0:
	  self.table[key][0] = out_values
	else:
	  self.table[key][0] = values[-1:]

    for key in self.index.keys():
      if len(self.index[key][0]) == 1 and self.index[key][0][0][0] > 0:
        continue
      elif len(self.index[key][0]) == 1:
        del self.index[key]
      elif len(self.index[key][0]) > 1:
        values = self.index[key][0]
	out_values = []
	for value in values:
          if value[1] > time:
            out_values.append(value)
	if len(out_values) > 0:
	  self.index[key][0] = out_values
	else:
	  self.index[key][0] = values[-1:]

    self.purge_stamp = datetime.datetime.now()

  def debug(self):
    """Print table and index dictionaries for debugging purposes."""
    print "TABLE"
    print self.table
    print "INDEX"
    print self.index


class TTDB(object):
  """A full TT database and network interface.

  Attributes:
    sock: The Unix socket to listen to for incoming connections
    connections: List of socket connections to listen to
    transactions: Dictionary mapping sockets to their open transactions
    ttable: TTDBTable object with the highest-level database
    purge_period: Minimum period at which to purge database of outdated items
  """
  def __init__(self, sock_addr='./ttdb_socket', purge_period=20):
    """Init TTDB with default Unix socket and purge period
    
    Args:
      sock_addr: Location of Unix socket to use
      purge_period: Minimum period at which to purge database of outdated items
    """
    try:
      os.unlink(sock_addr)
    except OSError:
      if os.path.exists(sock_addr):
        raise
    self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    self.sock.bind(sock_addr)
    self.sock.listen(5)
    self.connections = [self.sock]

    self.transactions = {}
    self.purge_period = purge_period
    self.ttable = TTDBTable(purge_period=self.purge_period)

  def run(self):
    """Run TTDB server on infinite listening loop."""
    while True:
      rready, wready, xready = select.select(self.connections, [], [], self.purge_period)

      for s in rready:
        if s == self.sock:
          connection, client_addr = self.sock.accept()
          self.connections.append(connection)
          print >>sys.stderr, "New connection: %d" % connection.fileno()
        else:
          data = s.recv(64)  # I'll have to robustify this in a bit
          if data:
            data = data.split('|')
            for datum in data:
              if len(datum) == 0:
                continue
              datum = datum.split()
              if datum[0] == 'SET' and len(datum) == 3:
                self.set(datum[1], datum[2], s)
              elif datum[0] == 'GET' and len(datum) == 2:
                self.get(datum[1], s)
              elif datum[0] == 'UNSET' and len(datum) == 2:
                self.unset(datum[1], s)
              elif datum[0] == 'NUMEQUALTO' and len(datum) == 2:
                self.numequalto(datum[1], s)
              elif datum[0] == 'BEGIN' and len(datum) == 1:
                self.begin(s)
              elif datum[0] == 'ROLLBACK' and len(datum) == 1:
                self.rollback(s)
              elif datum[0] == 'COMMIT' and len(datum) == 1:
                self.commit(s)
              elif datum[0] == 'RESET' and len(datum) == 1:
                self.ttable = TTDBTable()
                self.transactions = {}
		s.sendall('success')
              elif datum[0] == 'DEBUG' and len(datum) == 1:
                if s in self.transactions:
                  self.transactions[s].debug()
                else:
                  self.ttable.debug()
          else:
            s.close()
            self.connections.remove(s)
            if s in self.transactions:
              del self.transactions[s]
            print >>sys.stderr, "Connections: %s" % ",".join([str(i.fileno()) for i in self.connections])
      self.ttable.purge_entries(min([s.timestamp for s in self.transactions.values()] + [datetime.datetime.now()]))

  def begin(self, connection):
    """Open a new transaction and associate it with the connection.

    If the connection already has a transaction, this will nest a new one in it.

    Args:
      connection: The socket connection calling the 'begin'
    """
    if connection in self.transactions:
      self.transactions[connection].begin()
    else:
      self.transactions[connection] = TTDBTransaction(self.ttable)
    connection.sendall('success')

  def commit(self, connection):
    """Commit the open transaction, collapsing nested transactions if they exist.

    If this transaction is not the earliest existing, abort.  The earliest
    transaction has an implicit write lock to preserve consistency.

    Sends a message to connection to indicate success or failure.

    Args:
      connection: The socket connection calling the 'commit'
    """
    if connection in self.transactions and self.transactions[connection].timestamp > min([X.timestamp for X in self.transactions.values()]):
      del self.transactions[connection]
      print >>sys.stderr, 'sending abort'
      connection.sendall('Conflicting lock. Aborting transaction.')
      return
    if connection in self.transactions:
      success = self.transactions[connection].commit()
      del self.transactions[connection]
      if success:
        connection.sendall('success')
      else:
        connection.sendall('Commit failed. Rolling back.')
    else:
        connection.sendall('No transaction to commit.')

  def rollback(self, connection):
    """Rollback the open transaction, collapsing nested transactions if they exist.
    Sends a message to connection to indicate success or failure.

    Args:
      connection: The socket connection calling the 'rollback'
    """
    if connection in self.transactions:
      if self.transactions[connection].rollback() is None:
        del self.transactions[connection]
      connection.sendall('success')
    else:
      connection.sendall('INVALID ROLLBACK')

  def set(self, variable, value, connection):
    """Set variable to given the value

    If connection has an open transaction, the set falls through to it.
    Else if connection does not have an open transactions but another
    connection does abort as the earliest existing transaction has an implicit
    write lock.
    Else write value to variable in the main database

    Sends a message to connection to indicate success or failure.

    Args:
      variable: A string containing the variable to set
      value: The value to set variable to
      connection: The socket connection calling the 'set'
    """
    if connection in self.transactions:
      self.transactions[connection].set(variable, value)
    elif len(self.transactions) > 0:
      print >>sys.stderr, 'sending abort'
      connection.sendall('Conflicting lock. Aborting write.')
    else:
      self.ttable.write_value(variable, value, datetime.datetime.now())
    connection.sendall('success')

  def get(self, variable, connection):
    """Get current value of variable

    If connection has an open transaction, the get falls through to it.
    Else get value from the main database

    Sends a message to connection to indicate returned value

    Args:
      variable: A string containing the variable to get
      connection: The socket connection calling the 'get'
    """
    value = None
    if connection in self.transactions:
      value = self.transactions[connection].get(variable)
    else:
      value = self.ttable.read_value(variable, datetime.datetime.now())

    if value is None:
      value = 'NULL'
    connection.sendall(str(value))

  def unset(self, variable, connection):
    """Unset given variable

    If connection has an open transaction, the unset falls through to it.
    Else if connection does not have an open transactions but another
    connection does abort as the earliest existing transaction has an implicit
    write lock.
    Else unset value in the main database

    Sends a message to connection to indicate success or failure.

    Args:
      variable: A string containing the variable to unset
      connection: The socket connection calling the 'unset'
    """
    if connection in self.transactions:
      self.transactions[connection].unset(variable)
    elif len(self.transactions) > 0:
      print >>sys.stderr, 'sending abort'
      connection.sendall('Aborting write.')
    else:
      self.ttable.write_value(variable, None, datetime.datetime.now())
    connection.sendall('success')

  def numequalto(self, value, connection):
    """Get number of variables equal to value

    If connection has an open transaction, the numequalto falls through to it.
    Else get count from the main database

    Sends a message to connection to indicate returned count

    Args:
      value: A string containing the value to count
      connection: The socket connection calling the 'numequalto'
    """
    if connection in self.transactions:
      num = self.transactions[connection].numequalto(value)
    else:
      num = self.ttable.read_index(value, datetime.datetime.now())

    connection.sendall(str(num))


class TTDBTransaction(object):
  """A TTDB transaction containing its own subtable

  Attributes:
    ttable: TTDBTable object with the transaction-level database
    timestamp: datetime stamp indicating time at which the transaction was created and at which it acts
    subtransaction: TTDBTransaction object indicating the next level of transaction nesting
  """
  def __init__(self, parent, timestamp=None):
    """Init TTDBTransaction with given parent, a timestamp, and no subtransaction

    Args:
      parent: TTDBTable that acts as a parent to this transaction's table
      timestamp: Optional datetime stamp to use for read and write stamps from this transaction.  One should always be given when nesting and one will be created when not nesting.
    """
    if timestamp is None:
      self.timestamp = datetime.datetime.now()
    else:
      self.timestamp = timestamp
    self.subtransaction = None
    self.ttable = TTDBTable(parent)

  def begin(self):
    """Open a new subtransaction.

    If the transaction already has a subtransaction, this will nest a new one
    in it.
    """
    if self.subtransaction is not None:
      self.subtransaction.begin()
    else:
      self.subtransaction = TTDBTransaction(self.ttable, self.timestamp)

  def rollback(self):
    """Rollback the transaction, collapsing nested transactions if they exist."""
    if self.subtransaction is None:
      return None
    else:
      self.subtransaction = self.subtransaction.rollback()
      return self

  def commit(self):
    """Commit the transaction, collapsing nested transactions if they exist.

    The commit may fail if there is a conflicting read timestamp.
    
    Returns:
      A boolean indicating whether the commit succeeded.
    """
    success = True
    if self.subtransaction is not None:
      success = self.subtransaction.commit()

    if success:
      return self.ttable.commit()
    else:
      return False

  def set(self, variable, value):
    """Set variable to given the value

    If transaction has a subtransaction, the set falls through to it.
    Else write value to variable in the transaction database

    Args:
      variable: A string containing the variable to set
      value: The value to set variable to
    """
    if self.subtransaction is not None:
      self.subtransaction.set(variable, value)
    else:
      self.ttable.write_value(variable, value, self.timestamp)

  def get(self, variable):
    """Get current value of variable

    If connection has a subtransaction, the get falls through to it.
    Else get value from this trainsaction

    Args:
      variable: A string containing the variable to get

    Returns:
      The value of variable
    """
    if self.subtransaction is not None:
      return self.subtransaction.get(variable)
    else:
      return self.ttable.read_value(variable, self.timestamp)

  def unset(self, variable):
    """Unset given variable

    If connection has a subtransaction, the unset falls through to it.
    Else unset value in the transaction database

    Args:
      variable: A string containing the variable to unset
    """
    if self.subtransaction is not None:
      self.subtransaction.unset(variable)
    else:
      self.ttable.write_value(variable, None, self.timestamp)

  def numequalto(self, value):
    """Get number of variables equal to value

    If connection has a subtransaction, the numequalto falls through to it.
    Else get count from the transaction's index

    Args:
      value: A string containing the value to count

    Returns:
      The number of variables equal to value
    """
    if self.subtransaction is not None:
      return self.subtransaction.numequalto(value)
    else:
      return self.ttable.read_index(value, self.timestamp)

  def debug(self):
    """Print table and index dictionaries for debugging purposes."""
    if self.subtransaction is not None:
      self.subtransaction.debug()

    self.ttable.debug()


def main():
  parser = argparse.ArgumentParser(description='TTDB database server.')
  parser.add_argument('--socket', default='./ttdb_socket', help='location of Unix socket to connect to (default: ./ttdb_socket)')
  parser.add_argument('--pp', type=int, default=20, help='minimum time (in seconds) to wait before purging outdated entries (default: 20)')
  args = parser.parse_args()
  db = TTDB(sock_addr=args.socket, purge_period=args.pp)
  db.run()

if __name__ == '__main__':
  main()
