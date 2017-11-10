'''EXASolution Python Package

Copyright (c) 2004-2017 EXASOL AG. All rights reserved.

==========================
EXASolution Python Package
==========================

The EXASolution Python Package offers functionality to interact with the EXASolution
database out of Python scripts. It is developed as a wrapper around PyODBC and
extends PyODBC in two main aspects:

1. It offers fast data transfer between EXASolution and Python, multiple
   times faster than PyODBC.  This is achieved by using a proprietary
   transfer channel which is optimized for batch loading. Please read
   the Python help of exasol.readData() and exasol.writeData() for more
   details and read the documentation below.

2. It makes it convenient to run parts of your Python code in parallel on
   the EXASolution database, using EXASolution Python UDF scripts behind
   the scenes. For example you can define an Python function and execute it
   in parallel on different groups of data in an EXASolution
   table. Please read the Python help of exasol.createScript() function for
   more details and read the documentation below.


Prerequisites and Installation
------------------------------

1. Make sure you have ODBC and EXASolution ODBC installed and configured on
   your system. We recommend to create a DSN pointing to your database
   instance. Read the README of the EXASolution ODBC driver package for details.

2. Install a recent version of the PyODBC package.

3. Recommended: Install a recent version of the Pandas package, which is
   recommended but not required.

4. Install the EXASolution Python package using the following command:
   > python setup.py install

To get more information, use the python ``help'' function on the
package.



Importing the package
---------------------

To use the package import it with a handy name:

>>> import exasol as E

You can than read the documentation of this package with:

>>> help(E)



Connecting to EXASolution
-------------------------

The ``E.connect'' function has the same arguments, like
``pyodbc.connect'', with some additions. Please refer the PyODBC
documentation for connection parameters. To use it with EXASolution,
following arguments are possible:

Assuming you have a DSN pointing to your database instance you can
connect like this:

>>> C = E.connect(dsn='YourDSN')

Alternatively if you don't have a DSN you can also specify the required
information in the connection string:

>>> C = E.connect(Driver = 'libexaodbc-uo2214.so',
...               EXAHOST = 'exahost:8563',
...               EXAUID = 'sys',
...               EXAPWD = 'exasol)

The resulting object supports ``with'' statement, so the ``C.close''
function is called automatically on leaving the scope.



Executing queries
-----------------

The connection object has along with all PyODBC methods also a
``readData'' method, which executes the query through PyODBC but
receive the resulting data faster and in different formats. Currently
supported are Pandas and CSV, but it is possible to define arbitrary
reader functions. This function will be called inside of readData
with a file descriptor as argument, where the result need to be read
as CSV.

To use this function call it with the SQL:

>>> R = C.readData("SELECT * FROM MYTABLE")

The result type is a Pandas data frame per default. You can use a
different callback function using the argument readCallback, for
example you can use the predefined csvReadCallback to receive the
results formatted as CSV:
>>> R = C.readData("SELECT * FROM MYTABLE", readCallback = E.csvReadCallback)

We also offer an explicit function to read as CSV:
>>> R = C.readCSV("SELECT * FROM MYTABLE")

You can also change the default return type to CSV for the whole
connection using the following argument:
>>> C = E.connect(dsn="YourDSN", useCSV=True)



Write data to database
----------------------

With the function ``C.writeData'' python data can be transferred to
EXASolution database:

>>> C.writeData(R, table = 'mytable')

The data will be simply appended to the given table.
Similar to readData, the default format is a pandas data frame, which
can be changed using the writeCallback parameter or the explicit version:

>>> C.writeCSV(R, table = 'mytable')



Using User Defined Functions
----------------------------

With the function decorator ``createScript'' it is possible, to
declare python functions as EXASolution UDF scripts:

>>>  @C.createScript(inArgs = [('a', E.INT)],
...                  outArgs = [('b', E.INT), ('c', E.INT)])
...  def testScript(data):
...      print "process data", repr(ftplib)
...      while True:
...          data.emit(data.a, data.a + 3)
...          if not data.next(): break
...      print "all data processed"

This script will be immediatly created on the EXASolution database as
a UDF script and the local ``testScript'' function will be
replaced with a ``C.readData'' call, so that to execute the computation
on EXASolution you call this function simply as follows:

>>> testScript('columnA', table = 'testTable', groupBy = 'columnB')

This call executes a ``SELECT'' SQL query using the ``C.readData'' function
and returns the result. The query will group by columnB and aggregate on
the columnA column using the testScript function.

Per default, functions are created as SET EMITS UDFs. We recommend to read the
EXASolution manual about UDF scripts for a better understanding.

Internally the decorated function will be compiled and serialized with
the ``marshall'' Python module locally and created on the EXASolution
side, so that this function has no access to the local environment
anymore. To initialize the environment, it is possible to pass the
``initFunction'' argument of the decorator, which initializes the
environment on the EXASolution side. It happens every time the module
is loaded, so that this function is recreated in the database on
module loading.



'''

import sys, os, string, random, pyodbc
import socket, struct, marshal, zlib
import asyncore, asynchat, csv
#import traceback, time

from SocketServer import TCPServer
from threading import Thread, Lock, Event
from BaseHTTPServer import BaseHTTPRequestHandler

__author__ = 'EXASOL AG <support@exasol.com>'
__version__ = '6.0.1'
__date__ = '2017-06-19'

SET = "SET"
SCALAR = "SCALAR"
EMITS = "EMITS"
RETURNS = "RETURNS"

DECIMAL = lambda a,b: "DECIMAL(%d,%d)" % (a,b)
INT = "INT"
INTEGER = "INTEGER"
DOUBLE = "DOUBLE"
CHAR = lambda a: "CHAR(%d)" % (a)
VARCHAR = lambda a: "VARCHAR(%d)" % (a)
DATE = "DATE"
TIMESTAMP = "TIMESTAMP"

__all__ = (
    "SET",
    "SCALAR",
    "EMITS",
    "RETURNS",
    "DECIMAL",
    "INT",
    "INTEGER",
    "DOUBLE",
    "CHAR",
    "VARCHAR",
    "DATE",
    "TIMESTAMP",
    'connect',
    'pandasReadCallback',
    'pandasWriteCallback',
    'csvReadCallback',
    'csvWriteCallback',
    'outputService',
    )

if sys.version_info < (2,4) or sys.version_info >= (2,8):
    print sys.version_info, sys.version_info < (2,4), sys.version_info >= (2,8)
    raise RuntimeError("This package requires at least Python 2.4 and does not support Python 3.x")

class TunneledTCPServer(TCPServer):
    def server_bind(self):
        self.socket.connect(self.server_address)
        self.socket.sendall(struct.pack("iii", 0x02212102, 1, 1))
        _, self.proxyPort, host = struct.unpack("ii16s", self.socket.recv(24))
        self.proxyHost = host.replace('\x00', '')
    def handle_timeout(self): self.gotTimeout = True
    def server_activate(self): pass
    def get_request(self):
        #sys.stderr.write('@@@ get request called\n')
        return (self.socket, self.server_address)
    def shutdown_request(self, request): pass
    def close_request(self, request): pass

class HTTPIOHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass
    def do_PUT(self):
        data = None
        while True:
            line = self.rfile.readline().strip()
            if len(line) == 0: chunklen = 0
            else: chunklen = int(line, 16)
            if chunklen == 0:
                #sys.stderr.write('@@@ got eof, last data: %s\n' % repr(data))
                self.server.pipeOut.close()
                break
            data = self.rfile.read(chunklen)
            self.server.pipeOut.write(data)
            if self.rfile.read(2) != '\r\n':
                self.server.pipeOut.close()
                self.server.error = RuntimeError('Got wrong chunk delimiter in HTTP')
                break
        self.send_response(200, 'OK')
        self.end_headers()
    def do_GET(self):
        try:
            self.protocol_version = 'HTTP/1.1'
            self.send_response(200, 'OK')
            self.send_header('Content-type', 'application/octet-stream')
            self.send_header('Content-disposition', 'attachment; filename=data.csv')
            self.send_header('Connection', 'close')
            self.end_headers()
            self.server.startedEvent.set()
            while True:
                data = self.server.pipeIn.read(65535)
                if data is None or len(data) == 0: break
                self.wfile.write(data)
                self.wfile.flush()
        finally: self.server.doneEvent.set()

class HTTPIOServerThread(Thread):
    def run(self):
        #sys.stderr.write('@@@ start server\n')
        try:
            self.srv.timeout = 1
            while True:
                self.srv.gotTimeout = False
                self.srv.handle_request()
                if self.srv.error is not None:
                    if self.srv.outputMode:
                        self.srv.pipeOut.close()
                    break
                if not self.srv.gotTimeout: break
        except Exception, err:
            #traceback.print_exc()
            self.srv.error = err

class HTTPExportQueryThread(Thread):
    def run(self):
        try:
            fname = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(32)) + '.csv'
            self.odbc.execute("""EXPORT (%s) INTO CSV AT 'http://%s:%d' FILE '%s' WITH COLUMN NAMES""" % \
                              (self.sqlCommand, self.srv.proxyHost, self.srv.proxyPort, fname))
        except Exception, err:
            #traceback.print_exc()
            self.srv.error = err

class HTTPImportQueryThread(Thread):
    def run(self):
        try:
            fname = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(32)) + '.csv'
            columnNames = ""
            if self.columnNames:
                columnNames = "(%s)" % ", ".join(self.columnNames)
            self.odbc.execute("""IMPORT INTO %s%s FROM CSV AT 'http://%s:%d' FILE '%s'""" % \
                              (self.tableName, columnNames, self.srv.proxyHost, self.srv.proxyPort, fname))
        except Exception, err:
            #traceback.print_exc()
            self.srv.error = err

class ScriptOutputThread(Thread):
    def init(this):
        class log_server(asyncore.dispatcher):
            def __init__(self):
                asyncore.dispatcher.__init__(self)
                self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
                self.bind(this.serverAddress)
                if this.serverAddress[1] == 0:
                    this.serverAddress = (this.serverAddress[0], self.socket.getsockname()[1])
                self.listen(10)
            def handle_accept(self):
                log_handler(*self.accept())
            def handle_close(self):
                self.close()

        class log_handler(asynchat.async_chat):
            def __init__(self, sock, address):
                asynchat.async_chat.__init__(self, sock = sock)
                self.set_terminator("\n")
                self.address = "%s:%d" % address
                self.ibuffer = []
            def collect_incoming_data(self, data):
                self.ibuffer.append(data)
            def found_terminator(self):
                this.fileObject.write("%s> %s\n" % (self.address, ''.join(self.ibuffer).rstrip()))
                self.ibuffer = []

        this.serv = log_server()

    def run(self):
        try:
            while not self.finished:
                asyncore.loop(timeout = 1, count = 1)
        finally:
            self.serv.close()
            del self.serv
            asyncore.close_all()


def pandasReadCallback(inputFile, **kw):
    """Read callback for Pandas data frames"""
    # import only when required
    global pandas; import pandas  # pylint: disable=F0401
    return pandas.read_csv(inputFile, skip_blank_lines=False, **kw)

def pandasWriteCallback(data, outputFile, **kw):
    """Write callback for Pandas data frames"""
    # import only when required
    global pandas; import pandas # pylint: disable=F0401
    if not isinstance(data, pandas.DataFrame):
        raise TypeError("pandas.DataFrame expected as first argument")
    data.to_csv(outputFile, header = False, index = False, quoting = csv.QUOTE_NONNUMERIC, **kw)

def csvReadCallback(inputFile, **kw):
    """Read callback for CSV data"""
    inputFile.readline() # skip header
    reader = csv.reader(inputFile, lineterminator = '\n', **kw)
    return [row for row in reader]

def csvWriteCallback(data, outputFile, **kw):
    """Write callback for CSV data"""
    writer = csv.writer(outputFile, quoting = csv.QUOTE_MINIMAL, lineterminator = '\n', **kw)
    for row in data:
        writer.writerow(row)

class connect(object):
    """PyODBC compatible Connection class from exasol

    This connection class implements several enhancements to PyODBC
    for features, which are not well supported by PyODBC. This class
    wraps the PyODBC connection class and gives additional
    functionality to it. The PyODBC connection object is still
    available and the constructor has the same arguments as PyODBC:

    >>> import exasol as E
    >>> C = E.connect(DSN = 'test')
    >>> C.odbc  # underlying PyODBC object

    All PyODBC attributes are also directly accessible, f.e.:

    >>> C.execute('OPEN SCHEMA test')

    is equivalent to:

    >>> C.odbc.execute('OPEN SCHEMA test')

    """

    def __init__(self, *args, **kw):
        """This constructor transfers all arguments to the PyODBC constructor,
but has the following additions:

  clientAddress
    This keyword sets the host and port of the local machine for the
    output service in the form
      ('somehost.com', 5000)

    If the host is None, then the current hostname is
    used. Additionally with the keyword outputFile is it possible, to
    redirect the output to a spectific file object, instead of
    sys.stdout.

  externalClient
    Expect already running output service on address given with
    clientAddress.

  scriptSchema
    Database schema, which is used per default for scripts, if no
    'name' keyword given.

  useCSV
    Use instead of pandasReadCallback/pandasWriteCallback the
    csvReadCallback/csvWriteCallback per default. Alternatively it is
    possible to use specialized read/write functions, like readCSV or
    writePandas.

  serverAddress
    This keyword specifies the hostname and port of EXASolution RDBMS,
    per default got from PyODBC.

  EXAHOST
    The hostname or connection of EXASolution as string.

  EXAPORT
    The EXASolution port as int.

  EXAUID
    Username.

  EXAPWD
    Password.

        """

        self._connected = False
        if 'clientAddress' in kw:
            host, port = kw['clientAddress']
            if host is None:
                host = socket.gethostbyname(socket.gethostname())
            self.clientAddress = (str(host), int(port))
            del kw['clientAddress']
        else: self.clientAddress = None
        if 'externalClient' in kw:
            self.externalClient = kw['externalClient']
            del kw['externalClient']
        else: self.externalClient = False
        if 'useCSV' in kw:
            self.csvIsDefault = kw['useCSV']
            del kw['useCSV']
        else: self.csvIsDefault = False
        if 'serverAddress' in kw:
            host, port = kw['serverAddress']
            self.serverAddress = (str(host), int(port))
            del kw['serverAddress']
        else: self.serverAddress = None
        if 'outputFile' in kw:
            self.outputFileObject = kw['outputFile']
            del kw['outputFile']
        else: self.outputFileObject = sys.stdout
        if 'scriptSchema' in kw:
            self.scriptSchema = kw['scriptSchema']
            del kw['scriptSchema']
        else: self.scriptSchema = None

        self.odbc = pyodbc.connect(*args, **kw)
        if self.serverAddress is None:
            host, port = tuple(self.odbc.getinfo(pyodbc.SQL_SERVER_NAME).split(':'))
            self.serverAddress = (str(host), int(port))

        self.error = None
        self._outputService = None
        self._connected = True
        if self.clientAddress != None and not self.externalClient:
            self._startOutputService()
        self._outputLock = Lock()
        self._q = lambda x, q: q and '"%s"' % str(x).replace('"', '""') or str(x)

    def __enter__(self):
        """Allows to use E.connect in "with" statements"""
        if not self._connected: raise pyodbc.ProgrammingError("Not connected")
        return self
    def __exit__(self, type, value, tb):
        if not self._connected: return
        self.close()
    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        return getattr(self.__dict__['odbc'], name)
    def __del__(self):
        if self._connected:
            try: self.close()
            except: pass

    def _startOutputService(self):
        """Start service for EXASolution UDF scripts' output

        After the service is running, the createScript function
        produces additional code in scripts, which redirects the
        stdout and stderr of a stript to this service.

        The output of this service is the local stdout.

        """
        if not self._connected: raise pyodbc.ProgrammingError("Not connected")
        self._stopOutputService()
        self._outputService = ScriptOutputThread()
        self._outputService.fileObject = self.outputFileObject
        self._outputService.finished = False
        self._outputService.serverAddress = self.clientAddress
        self._outputService.init()
        self.clientAddress = self._outputService.serverAddress
        self._outputService.start()

    def _stopOutputService(self):
        """Stop service for EXASolution UDF scripts' output

        After stopping the output service, the scripts, which was
        created with running output service, will not work any more.

        """
        if self._outputService is None: return
        try:
            self._outputService.finished = True
            self._outputService.join()
        finally: self._outputService = None

    def readData(self, sqlCommand, readCallback = None, **kw):
        """Execute a DQL statement and returns the result

        This is a optimized version of pyodbc.Connection.execute
        function. ReadData returns per default a pandas data frame
        or any other data, if a different readCallback was specified.

          readCallback
            A function, which is called with the file object contained
            the query result as CSV and all keyword arguments given to
            readData. The returned data will be returned from
            readData function.

        """
        if not self._connected: raise pyodbc.ProgrammingError("Not connected")
        if readCallback is None:
            if self.csvIsDefault:
                readCallback = csvReadCallback
            else: readCallback = pandasReadCallback
        odbc = self.odbc; self.odbc = None # during command execution is odbc not usable
        try:
            srv = TunneledTCPServer(self.serverAddress, HTTPIOHandler)
            srv.pipeInFd, srv.pipeOutFd = os.pipe(); srv.outputMode = True
            srv.error, srv.pipeIn, srv.pipeOut = None, os.fdopen(srv.pipeInFd), os.fdopen(srv.pipeOutFd, 'w')
            s = HTTPIOServerThread();    s.srv = srv; srv.serverThread = s
            q = HTTPExportQueryThread(); q.srv = srv; srv.queryThread = q
            q.sqlCommand = sqlCommand
            q.odbc = odbc
            s.start(); q.start()

            try:
                try:
                    ret = readCallback(s.srv.pipeIn, **kw)
                except Exception, err:
                    if srv.error is not None:
                        raise srv.error
                    #traceback.print_exc()
                    raise err
            finally:
                srv.server_close()
                try: srv.pipeIn.close()
                except: pass
                try: srv.pipeOut.close()
                except: pass
                q.join(); s.join()
        finally: self.odbc = odbc
        if srv.error is not None:
            raise srv.error
        return ret

    def readCSV(self, *args, **kw):
        """Shortcut to readData(..., readCallback = csvReadCallback)"""
        kw['readCallback'] = csvReadCallback
        return self.readData(*args, **kw)

    def readPandas(self, *args, **kw):
        """Shortcut to readData(..., readCallback = pandasReadCallback)"""
        kw['readCallback'] = pandasReadCallback
        return self.readData(*args, **kw)

    def writeData(self, data, table,
                   columnNames = None,
                   quotedIdentifiers = False,
                   writeCallback = None,
                   **kw):
        """Import data to a table in EXASolution DBMS

        Per default it imports the given pandas data frame to the
        given table. If a writeCallback is specified, then this
        function is called with given data frame and a file object,
        where the CSV file should be written. The format of CSV should
        be csv.excel dialect.

        """
        if not self._connected: raise pyodbc.ProgrammingError("Not connected")
        if writeCallback is None:
            if self.csvIsDefault:
                writeCallback = csvWriteCallback
            else: writeCallback = pandasWriteCallback
        odbc = self.odbc; self.odbc = None
        try:
            srv = TunneledTCPServer(self.serverAddress, HTTPIOHandler)
            srv.pipeInFd, srv.pipeOutFd = os.pipe(); srv.outputMode = False
            srv.doneEvent = Event(); srv.startedEvent = Event()
            srv.error, srv.pipeIn, srv.pipeOut = None, os.fdopen(srv.pipeInFd), os.fdopen(srv.pipeOutFd, 'w')
            s = HTTPIOServerThread();    s.srv = srv; srv.serverThread = s
            q = HTTPImportQueryThread(); q.srv = srv; srv.queryThread = q
            q.tableName = self._q(table, quotedIdentifiers)
            q.columnNames = None
            if columnNames is not None:
                q.columnNames = [self._q(c, quotedIdentifiers) for c in columnNames]
            q.odbc = odbc
            s.start(); q.start()
            if 'columnNames' in kw: del kw['columnNames']
            if 'quotedIdentifiers' in kw: del kw['quotedIdentifiers']
            if 'writeCallback' in kw: del kw['writeCallback']

            try:
                try:
                    while not srv.startedEvent.wait(1):
                        if srv.error is not None:
                            srv.doneEvent.set()
                            raise RuntimeError("Server error")
                    ret = writeCallback(data, srv.pipeOut, **kw)
                except Exception, err:
                    if srv.error is not None:
                        raise srv.error
                    #traceback.print_exc()
                    try: srv.pipeIn.close()
                    except: pass
                    raise err
            finally:
                try: srv.pipeOut.close()
                except: pass
                srv.doneEvent.wait()
                srv.server_close()
                s.join()
                q.join()
        finally: self.odbc = odbc
        if srv.error is not None:
            raise srv.error
        return ret

    def writeCSV(self, *args, **kw):
        """Shortcut to writeData(..., writeCallback = csvWriteCallback)"""
        kw['writeCallback'] = csvWriteCallback
        return self.writeData(*args, **kw)

    def writePandas(self, *args, **kw):
        """Shortcut to writeData(..., writeCallback = pandasWriteCallback)"""
        kw['writeCallback'] = pandasWriteCallback
        return self.writeData(*args, **kw)

    def createScript(self,
                     name = None,
                     env = None,
                     initFunction = None,
                     cleanFunction = None,
                     replaceScript = True,
                     quotedIdentifiers = False,
                     inType = SET,
                     inArgs = [],
                     outType = EMITS,
                     outArgs = []):
        """Converts a Python function to EXASolution UDF script

        This function decorator converts a regular python function to
        an EXASolution UDF script, which is created in connected
        EXASolution RDMBS. The modified function runs then in the
        EXASolution RDMBS context in multiple parallel instances,
        therefore the function has no access to local context of
        E.connect. To import modules or prepare the context for the
        function please set the initFunction and do it there.

        It has following keyword arguments:

          name

            The script name to use in the database, default is the
            python name of the function

          env

            A dictionary with variable names as keys and variable
            content as values, which should be defined when the script
            is started

          initFunction

            A function which is called on initialization. All contex
            changes, which should be available in the modified
            function need to be done here and defined as global:

              def myInit():
                global ftplib
                import ftplib

          cleanFunction

            This function will be called to clean up the context of
            modified function, e.g. close connections or similar

          replaceScript = True

            If this keyword argument is True (default) then the script
            will be replaced on EXASolution side if already exists

          quotedIdentifiers = False

            If this keyword argument is True, then all identifiers in
            generated SQL will be quoted

          inType = SET

            The type of EXASolution UDF script, please refer the
            EXASolution documentation

          inArgs = []

            The input argumens as list of (name, type) tuples

          outType = EMITS

            The type of EXASolution UDF script, please refer the
            EXASolution documentation

          outArgs = []

            Output arguments of the EXASolution UDF script. If
            outType==EMITS, then the same format as with inArgs, but
            if outType==RETURNS, then only the SQL type name

        The modified function has then other arguments:

          fun(*args, # args should be a list of strings and need to
                     # correspond to inArgs
              table, # name of input table, is required
              where = None,              # the WHERE part of SQL
              groupBy = None,            # the GROUP BY part of SQL
              restQuery = '',            # rest of the QUERY (e.g. ORDER BY)
              quotedIdentifiers = False,
              returnSQL = False,         # on execute return only the SQL text
              **kw)                      # keywords to pass to readData

        If the modified function is called, then a query in the
        EXASolution DBMS is executed which applys the created script
        on the given table. The result is then returned in the same
        format as with readData.

        """
        if sys.version_info < (2,7) or sys.version_info >= (2,8):
            raise RuntimeError('createScript requires Python 2.7')
        if not self._connected: raise pyodbc.ProgrammingError("Not connected")
        qi = quotedIdentifiers
        def createPythonScript(function):
            if name is None:
                if self.scriptSchema is None:
                    scriptName = function.func_name
                else: scriptName = "%s.%s" % (self.scriptSchema,
                                              function.func_name)
            else: scriptName = name
            if qi: scriptName = '"%s"' % scriptName
            scriptCode = ["# AUTO GENERATED CODE FROM EXASOLUTION PYTHON PACKAGE"]
            scriptCode.append("import marshal, types, sys, socket, time, zlib")
            if env != None:
                scriptCode.append("env = marshal.loads(zlib.decompress(%s))" % \
                                  repr(zlib.compress(marshal.dumps(env), 9)))
            scriptCode.append("run = types.FunctionType(marshal.loads(zlib.decompress(%s)), globals(), %s)" % \
                              (repr(zlib.compress(marshal.dumps(function.func_code), 9)),
                               repr(function.func_name)))
            if cleanFunction is not None:
                scriptCode.append("cleanup = types.FunctionType(marshal.loads(zlib.decompress(%s)), globals(), %s)" % \
                                  (repr(zlib.compress(marshal.dumps(cleanFunction.func_code), 9)),
                                   repr(cleanFunction.func_name)))

            if self._outputService is not None or self.externalClient:
                serverAddress = self.clientAddress
                if self._outputService is not None:
                    serverAddress = self._outputService.serverAddress
                scriptCode.append("""# OUTPUT REDIRECTION
class activate_remote_output:
    def __init__(self, address):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect(address)
        sys.stdout = sys.stderr = self
    def write(self, data): return self.s.sendall(data)
    def close(self): self.s.close()
activate_remote_output(%s)""" % repr(serverAddress))

            if initFunction is not None:
                scriptCode.append("types.FunctionType(marshal.loads(%s), globals(), %s)()" % \
                                  (repr(marshal.dumps(initFunction.func_code)),
                                   repr(initFunction.func_name)))
            scriptCode = "\n".join(scriptCode)
            scriptReplace = ""
            if replaceScript:
                scriptReplace = "OR REPLACE"
            scriptInType = "SET"
            if inType == SCALAR:
                scriptInType = "SCALAR"

            if not isinstance(inArgs, basestring):
                scriptInArgs = []
                for n,t in inArgs:
                    scriptInArgs.append("%s %s" % (self._q(n, qi), t))
                scriptInArgs = ", ".join(scriptInArgs)
            else: scriptInArgs = inArgs

            if outType == RETURNS:
                if not isinstance(outArgs, basestring):
                    raise TypeError("outArgs need te be a string for outType == RETURNS")
                scriptOutArgs = outArgs
            else:
                if not isinstance(outArgs, basestring):
                    scriptOutArgs = []
                    for n,t in outArgs:
                        scriptOutArgs.append("%s %s" % (self._q(n, qi), t))
                    if len(scriptOutArgs) == 0:
                        raise RuntimeError("One or more output arguments required")
                    scriptOutArgs = '(' + ", ".join(scriptOutArgs) + ')'
                else: scriptOutArgs = '(' + outArgs + ')'

            scriptOutType = "EMITS"
            if outType == RETURNS:
                scriptOutType = "RETURNS"
            sqlCode = 'CREATE %s PYTHON %s SCRIPT %s (%s) %s %s AS\n%s\n' % \
                      (scriptReplace, scriptInType, scriptName, scriptInArgs, scriptOutType, scriptOutArgs, scriptCode)
            self.odbc.execute(sqlCode)

            def f(*args, **kw):
                if not self._connected:
                    raise pyodbc.ProgrammingError("Not connected")
                table = kw['table']
                where = kw.get('where', None)
                groupBy = kw.get('groupBy', None)
                restQuery = kw.get('restQuery', '')
                qis = kw.get('quotedIdentifiers', qi)
                returnSQL = kw.get('returnSQL', False)
                for k in ('table', 'where', 'groupBy', 'restQuery', 'quotedIdentifiers', 'returnSQL'):
                    if k in kw:
                        del kw[k]
                funargs = []
                for n in args:
                    funargs.append(self._q(n, qis))
                whereSQL = ""
                if where:
                    whereSQL = "WHERE %s" % where
                groupBySQL = ""
                if groupBy:
                    groupBySQL = "GROUP BY %s" % self._q(groupBy, qis)
                code = "SELECT * FROM (SELECT %s(%s) FROM %s %s %s) %s" % \
                       (scriptName, ", ".join(funargs), self._q(table, qis), whereSQL, groupBySQL, str(restQuery))
                if returnSQL: return '(%s)' % code
                return self.readData(code, **kw)
            f.func_name = function.func_name
            return f
        return createPythonScript

    def close(self):
        """Closes the underlying pyodbc.Connection object and stops
        any implicitly started output service."""
        if not self._connected:
            raise pyodbc.ProgrammingError("Not connected")
        self._connected = False
        try: self.odbc.close()
        finally: self._stopOutputService()

def outputService():
    """Start a standalone output service

    This service can be used in an other Python or R instance, for
    Python instances the connection parameter externalClient need to
    be specified.
    """
    try: host = socket.gethostbyname(socket.gethostname())
    except: host = '0.0.0.0'

    from optparse import OptionParser
    parser = OptionParser(description =
                          """This script binds to IP and port and outputs everything it gets from
                          the connections to stdout with all lines prefixed with client address.""")
    parser.add_option("-s", "--server", dest="server", metavar="SERVER", type="string",
                      default=host,
                      help="hostname or IP address to bind to (default: %default)")
    parser.add_option("-p", "--port", dest="port", metavar="PORT", type="int", default=3000,
                      help="port number to bind to (default: %default)")
    #(options, args) = parser.parse_args()
    options = parser.parse_args()[0]
    address = options.server, options.port
    sys.stdout.flush()
    server = ScriptOutputThread()
    server.serverAddress = address
    server.fileObject = sys.stdout
    server.finished = False
    server.init()
    print ">>> bind the output server to %s:%d" % server.serverAddress
    sys.stdout.flush()
    try: server.run()
    except KeyboardInterrupt:
        sys.stdout.flush()
    sys.exit(0)

if __name__ == '__main__':
    outputService()
