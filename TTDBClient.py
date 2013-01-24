#!/usr/bin/python2

import argparse
import socket
import sys

def main():
  parser = argparse.ArgumentParser(description='TTDB database client.')
  parser.add_argument('--socket', default='./ttdb_socket', help='location of Unix socket to connect to (default: ./ttdb_socket)')
  args = parser.parse_args()

  sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
  try:
    sock.connect(args.socket)
  except socket.error, msg:
    print >>sys.stderr, msg
    sys.exit(1)

  while True:
    line = sys.stdin.readline().split()
    if len(line) == 0:
      continue
    elif line[0].upper() == 'END' and len(line) == 1:
      sock.close()
      break
    elif line[0].upper() == 'SET' and len(line) == 3:
      do_set(line[1], line[2], sock)
    elif line[0].upper() == 'GET' and len(line) == 2:
      do_get(line[1], sock)
    elif line[0].upper() == 'UNSET' and len(line) == 2:
      do_unset(line[1], sock)
    elif line[0].upper() == 'NUMEQUALTO' and len(line) == 2:
      do_numequalto(line[1], sock)
    elif line[0].upper() == 'BEGIN' and len(line) == 1:
      do_begin(sock, 'RW')
    elif line[0].upper() == 'BEGIN' and len(line) == 2 and line[1].upper() in ['RW', 'RO']:
      do_begin(sock, line[1])
    elif line[0].upper() == 'ROLLBACK' and len(line) == 1:
      do_rollback(sock)
    elif line[0].upper() == 'COMMIT' and len(line) == 1:
      do_commit(sock)
    elif line[0].upper() == 'RESET' and len(line) == 1:
      do_reset(sock)
    elif line[0].upper() == 'DEBUG' and len(line) == 1:
      do_debug(sock)
    else:
      print 'Invalid syntax for command %s' % line[0]
    
def do_set(variable, value, sock):
  """Send SET command to server.

  Args:
    variable: variable to set
    value: value to which to set variable
    sock: socket connection where to send command
  """
  sock.sendall(" ".join(('SET', variable, value, '|')))
  msg = sock.recv(64)
  if msg != 'success':
    print msg

def do_get(variable, sock):
  """Send GET command to server.

  Args:
    variable: variable whose value to get
    sock: socket connection where to send command
  """
  sock.sendall(" ".join(('GET', variable, '|')))
  print sock.recv(64)

def do_unset(variable, sock):
  """Send UNSET command to server.

  Args:
    variable: variable to unset
    sock: socket connection where to send command
  """
  sock.sendall(" ".join(('UNSET', variable, '|')))
  msg = sock.recv(64)
  if msg != 'success': print msg

def do_numequalto(value, sock):
  """Send NUMEQUALTO command to server.

  Args:
    value: the value to count
    sock: socket connection where to send command
  """
  sock.sendall(" ".join(('NUMEQUALTO', value, '|')))
  print sock.recv(64)

def do_begin(sock, transaction_type):
  """Send BEGIN command to server.

  Args:
    sock: socket connection where to send command
  """
  if transaction_type.upper() == 'RW':
    sock.sendall('BEGIN RW |')
  elif transaction_type.upper() == 'RO':
    sock.sendall('BEGIN RO |')
  msg = sock.recv(64)
  if msg != 'success':
    print msg

def do_rollback(sock):
  """Send ROLLBACK command to server.

  Args:
    sock: socket connection where to send command
  """
  sock.sendall('ROLLBACK |')
  msg = sock.recv(64)
  if msg != 'success':
    print msg

def do_commit(sock):
  """Send COMMIT command to server.

  Args:
    sock: socket connection where to send command
  """
  sock.sendall('COMMIT |')
  msg = sock.recv(64)
  if msg != 'success':
    print msg

def do_reset(sock):
  """Send RESET command to server.

  Args:
    sock: socket connection where to send command
  """
  sock.sendall('RESET |')
  msg = sock.recv(64)
  if msg != 'success':
    print msg

def do_debug(sock):
  """Send DEBUG command to server.

  Args:
    sock: socket connection where to send command
  """
  sock.sendall('DEBUG |')
  msg = sock.recv(64)
  if msg != 'success':
    print msg

if __name__ == '__main__':
  main()
