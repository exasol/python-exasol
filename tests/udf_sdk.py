'''Test of executeSQL with CSV'''

import os
import signal
import socket
import subprocess
import sys
import threading
import time
import unittest

from decimal import Decimal
from textwrap import dedent
from cStringIO import StringIO

sys.path.append('/buckets/testsystem')

if 'SGE_NODES' in os.environ:
        import portreg.client

import pyodbc
import pandas

import exasol
from exasol import (
        SET, EMITS, RETURNS, SCALAR,
        INT, DECIMAL, DOUBLE, CHAR, VARCHAR,
        )


class TestCase(unittest.TestCase):
    longMessage = True

    def setUp(self):
        self.odbc_kwargs = {
                #'DSN':'EXAODBC_TEST',
                'Driver': 'EXAODBC',
                'EXAHOST': os.environ['ODBC_HOST'],
                'EXAUID': os.environ['EXAUSER'],
                'EXAPWD': os.environ['EXAPW']
                }
        if 'ODBC_LOG' in os.environ:
            self.odbc_kwargs['LOGFILE'] = os.environ['ODBC_LOG']

        with pyodbc.connect(**self.odbc_kwargs) as con:
            try:
                con.cursor().execute('CREATE SCHEMA foo')
            except pyodbc.ProgrammingError:
                # schema FOO exists
                pass

    def tearDown(self):
        with pyodbc.connect(**self.odbc_kwargs) as con:
            con.cursor().execute('DROP SCHEMA foo CASCADE')


class Defaults(TestCase):
    def test_createScript_defaults_to_pandas(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            ecn.execute('OPEN SCHEMA foo')

            @ecn.createScript(inArgs=[('a', INT)], outArgs=[('a', INT)])
            def foo(ctx):
                ctx.emit(int(ctx.a))

            self.assertIsInstance(foo(3.4, table='dual'), pandas.DataFrame)

    def test_createScript_set_default_with_connect(self):
        with exasol.connect(useCSV=True, **self.odbc_kwargs) as ecn:
            ecn.execute('OPEN SCHEMA foo')

            @ecn.createScript(inArgs=[('a', INT)], outArgs=[('a', INT)])
            def foo(ctx):
                ctx.emit(int(ctx.a))

            self.assertIsInstance(foo(3.4, table='dual'), list)

    def test_createScript_overwrite_default(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            ecn.execute('OPEN SCHEMA foo')

            @ecn.createScript(inArgs=[('a', INT)], outArgs=[('a', INT)])
            def foo(ctx):
                ctx.emit(int(ctx.a))

            self.assertIsInstance(
                    foo(3.4, table='dual',
                            readCallback=exasol.csvReadCallback),
                    list)

    def test_createScript_schema_default(self):
        with exasol.connect(scriptSchema='foo', **self.odbc_kwargs) as ecn:
            @ecn.createScript(inArgs=[('a', INT)], outArgs=[('a', INT)])
            def bar(ctx):
                pass

            rows = ecn.cursor().execute(dedent("""\
                    SELECT *
                    FROM EXA_ALL_SCRIPTS
                    WHERE script_name = 'BAR' and
                        script_schema = 'FOO'
                    """)).fetchall()
        self.assertEqual(1, len(rows))

    def test_createScript_name_overwrites_schema_default(self):
        with exasol.connect(scriptSchema='babelfish', **self.odbc_kwargs) as ecn:
            @ecn.createScript(name='foo.baz', inArgs=[('a', INT)], outArgs=[('a', INT)])
            def bar(ctx):
                pass

            rows = ecn.cursor().execute(dedent("""\
                    SELECT *
                    FROM EXA_ALL_SCRIPTS
                    WHERE script_name = 'BAZ' and
                        script_schema = 'FOO'
                    """)).fetchall()
        self.assertEqual(1, len(rows))


class SimpleFunctionality(TestCase):
    def test_createScript_works_set_emits(self):
        with exasol.connect(useCSV=True, **self.odbc_kwargs) as ecn:
            ecn.execute('OPEN SCHEMA foo')

            @ecn.createScript(
                    inArgs=[('a', DOUBLE)],
                    outArgs=[('a', INT)],
                    )
            def foo(ctx):
                ctx.emit(int(ctx.a))

            self.assertEqual([['3']], foo(3.4, table='dual'))

    def test_createScript_works_scalar_returns(self):
        with exasol.connect(useCSV=True, **self.odbc_kwargs) as ecn:
            ecn.execute('OPEN SCHEMA foo')

            @ecn.createScript(
                    inType=SCALAR,
                    inArgs=[('a', DOUBLE)],
                    outType=RETURNS,
                    outArgs=INT,
                    )
            def foo(ctx):
                return int(ctx.a)

            self.assertEqual([['3']], foo(3.4, table='dual'))

    def test_createScript_works_with_decimal(self):
        with exasol.connect(useCSV=True, **self.odbc_kwargs) as ecn:
            ecn.execute('OPEN SCHEMA foo')

            @ecn.createScript(
                    inArgs=[('a', DOUBLE)],
                    outArgs=[('a', DECIMAL(10,4))],
                    )
            def foo(ctx):
                ctx.emit(int(ctx.a))

            self.assertEqual([['3']], foo(3.4, table='dual'))

    def test_created_script_is_persistent(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            ecn.execute('OPEN SCHEMA foo')

            @ecn.createScript(
                    inArgs=[('a', INT)],
                    outArgs=[('a', INT)],
                    )
            def foobar(ctx):
                ctx.emit(4)

            ecn.commit()

        with pyodbc.connect(**self.odbc_kwargs) as con:
            self.assertEqual(1, len(con.cursor().execute(
                 "SELECT * FROM EXA_ALL_SCRIPTS WHERE script_name = 'FOOBAR'").fetchall()))


class DataTypes(TestCase):
    def create_script(self, type_):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            ecn.execute('OPEN SCHEMA foo')

            @ecn.createScript(
                    name="foobar",
                    inArgs=[('a', type_)],
                    outArgs=[('a', INT)],
                    )
            def foo(ctx):
                pass
            ecn.commit()

    def check_type(self, type_):
        with pyodbc.connect(**self.odbc_kwargs) as con:
            rows = con.cursor().execute(dedent("""\
                    SELECT script_text
                    FROM EXA_ALL_SCRIPTS
                    WHERE script_name = 'FOOBAR'
                    """)).fetchall()
        self.assertIn('("a" %s) EMITS' % type_, rows[0][0])


    def test_createScript_with_int(self):
        self.create_script(INT)
        self.check_type('DECIMAL(18,0)')

    def test_createScript_with_decimal(self):
        self.create_script(DECIMAL(6,4))
        self.check_type('DECIMAL(6,4)')

    def test_createScript_with_char(self):
        self.create_script(CHAR(5))
        self.check_type('CHAR(5) UTF8')

    def test_createScript_with_varchar(self):
        self.create_script(VARCHAR(20))
        self.check_type('VARCHAR(20) UTF8')


class Interface(TestCase):
    def create_script(self, inargs='a', outargs='a'):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            ecn.execute('OPEN SCHEMA foo')

            @ecn.createScript(
                    name="foobar",
                    inArgs=[(x, DOUBLE) for x in inargs],
                    outArgs=[(x, DOUBLE) for x in outargs],
                    )
            def foo(ctx):
                pass
            ecn.commit()

    def check_typespec(self, inargs='a', outargs='a'):
        with pyodbc.connect(**self.odbc_kwargs) as con:
            rows = con.cursor().execute(dedent("""\
                    SELECT script_text
                    FROM EXA_ALL_SCRIPTS
                    WHERE script_name = 'FOOBAR'
                    """)).fetchall()
        typespec = (
                '(' +
                ', '.join(['"%s" DOUBLE' % x for x in inargs]) +
                ') EMITS (' +
                ', '.join(['"%s" DOUBLE' % x.upper() for x in outargs]) +
                ') AS')

        self.assertIn(typespec, rows[0][0])

    def test_argorder_controllgroup(self):
        self.create_script('a', 'x')
        self.check_typespec('a', 'x')

    def test_inArgs_order_1(self):
        self.create_script('abc', 'x')
        self.check_typespec('abc', 'x')

    def test_inArgs_order_2(self):
        self.create_script('cab', 'x')
        self.check_typespec('cab', 'x')

    def test_outArgs_order_1(self):
        self.create_script('a', 'xyz')
        self.check_typespec('a', 'xyz')

    def test_outArgs_order_2(self):
        self.create_script('a', 'zxy')
        self.check_typespec('a', 'zxy')

    def test_createScript_local_argorder_args(self):
        with exasol.connect(useCSV=True, **self.odbc_kwargs) as ecn:
            ecn.execute('OPEN SCHEMA foo')

            @ecn.createScript(
                    inArgs=[
                            ('c', DOUBLE),
                            ('a', DOUBLE),
                            ('d', VARCHAR(20)),
                            ('b', VARCHAR(20)),
                            ],
                    outType=RETURNS,
                    outArgs=VARCHAR(40),
                    )
            def foo(ctx):
                return 'a=' + str(ctx.a) + '; b=' + str(ctx.b) + '; c=' + str(ctx.c) + '; d=' + str(ctx.d)

            result = foo(4.5, 1.3, "'foo'", "'bar'", table='dual')
            self.assertEqual('a=1.3; b=bar; c=4.5; d=foo', result[0][0])


class OutputService(TestCase):
    def setUp(self):
        super(self.__class__, self).setUp()
        self.script_kwargs = {
                    'inType': SCALAR,
                    'inArgs': [('a', VARCHAR(1024))],
                    'outType': RETURNS,
                    'outArgs': VARCHAR(1024),
                    }
        self.port = 5000
        if 'SGE_NODES' in os.environ:
            self.port = portreg.client.get_port()

    def tearDown(self):
        if 'SGE_NODES' in os.environ:
            portreg.client.del_port(self.port)

    def test_redirect_output_given_port(self):
        buffer = StringIO()
        with exasol.connect(
                clientAddress=(None, self.port),
                outputFile=buffer,
                scriptSchema='foo',
                useCSV=True,
                **self.odbc_kwargs) as ecn:

            @ecn.createScript(**self.script_kwargs)
            def echo(ctx):
                print ctx.a
                return 'no output'

            out = echo("'foobar'", table='dual')

        self.assertEqual('no output', out[0][0])
        self.assertIn('foobar', buffer.getvalue())

    def test_redirect_output_anyport(self):
        buffer = StringIO()
        with exasol.connect(clientAddress=(None, 0),
                outputFile=buffer,
                scriptSchema='foo',
                useCSV=True,
                **self.odbc_kwargs) as ecn:

            @ecn.createScript(**self.script_kwargs)
            def echo(ctx):
                print ctx.a
                return 'no output'

            out = echo("'foobar'", table='dual')

        self.assertEqual('no output', out[0][0])
        self.assertIn('foobar', buffer.getvalue())

class ExecBackground(threading.Thread):
    def __init__(self, *cmd):
        super(self.__class__, self).__init__()
        self.daemon = True
        self.cmd = cmd
        self._lock = threading.Lock()
        self.child = None
        self.output = None

    def run(self):
        with self._lock:
            self.child = subprocess.Popen(
                    self.cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    )
        out, err = self.child.communicate()
        #print 'out:', out
        #print 'err:', err
        with self._lock:
            self.output = out

    def stop(self):
        with self._lock:
            try:
                for sig in signal.SIGINT, signal.SIGTERM, signal.SIGKILL:
                    if self.child.poll() is not None:
                        break
                    self.child.send_signal(sig)
                    time.sleep(2)
            except OSError:
                # No such process
                pass
        self.join()


class ExternalOutputService(TestCase):
    def setUp(self):
        super(self.__class__, self).setUp()
        self.script_kwargs = {
                    'inType': SCALAR,
                    'inArgs': [('a', VARCHAR(1024))],
                    'outType': RETURNS,
                    'outArgs': VARCHAR(1024),
                    }
        self.port = 5000
        if 'SGE_NODES' in os.environ:
            self.port = portreg.client.get_port()
        self.exatoolbox = exasol.__file__
        self.interpreter = sys.executable

    def tearDown(self):
        if 'SGE_NODES' in os.environ:
            portreg.client.del_port(self.port)


    def test_start_external_service_given_port(self):
        eos = ExecBackground(self.interpreter, self.exatoolbox, '--port', str(self.port))
        eos.start()
        time.sleep(10)
        eos.stop()
        self.assertRegexpMatches(eos.output, r'bind .* to .*:%d' % self.port)

    def test_start_external_service_any_port(self):
        eos = ExecBackground(self.interpreter, self.exatoolbox, '--port=0')
        eos.start()
        time.sleep(10)
        eos.stop()
        self.assertRegexpMatches(eos.output, r'bind .* to .*:')
        self.assertNotRegexpMatches(eos.output, r'bind .* to .*:0')

    def xtest_start_external_service_get_data(self):
        eos = ExecBackground(self.interpreter, self.exatoolbox, '--port', str(self.port))
        eos.start()
        time.sleep(5)
        buffer = StringIO()
        with exasol.connect(
                clientAddress=(None, self.port),
                externalClient=True,
                outputFile=buffer,
                scriptSchema='foo',
                useCSV=True,
                **self.odbc_kwargs) as ecn:

            @ecn.createScript(**self.script_kwargs)
            def echo(ctx):
                print ctx.a
                return 'no output'

            out = echo("'foobar'", table='dual')
        eos.stop()

        self.assertEqual('no output', out[0][0])
        self.assertEqual('', buffer.getvalue())
        self.assertIn('foobar', eos.output)


class WithStatement(TestCase):

    def test_reraise_correct_exception_without_outputservice(self):
        with self.assertRaises(ZeroDivisionError):
            with exasol.connect(**self.odbc_kwargs) as ecn:
                raise ZeroDivisionError('Boom!')

    def test_reraise_correct_exception_with_outputservice(self):
        with self.assertRaises(ZeroDivisionError):
            with exasol.connect(clientAddress=(None, 0), **self.odbc_kwargs) as ecn:
                raise ZeroDivisionError('Boom!')

    def test_no_exception_in_any_thread(self):
        try:
            _sys_stderr = sys.stderr
            sys.stderr = StringIO()
            with exasol.connect(clientAddress=(None, 0), **self.odbc_kwargs) as ecn:
                pass
        finally:
            stderr = sys.stderr.getvalue()
            sys.stderr = _sys_stderr
        self.assertNotIn('Traceback', stderr)



if __name__ == '__main__':
    unittest.main(verbosity=2)

# vim: ts=4:sts=4:sw=4:et:fdm=indent
