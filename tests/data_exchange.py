"""Test of executeSQL with CSV"""

import sys
import operator
import os
import socket
import unittest
import random
from decimal import Decimal

import pyodbc
import pandas

import exasol

if sys.version_info[0] == 3:
    from functools import reduce


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

        with exasol.connect(**self.odbc_kwargs) as ecn:
            crs = ecn.cursor()
            crs.execute('CREATE SCHEMA IF NOT EXISTS exasol_travis_python')
            crs.execute('DROP TABLE IF EXISTS data_exchange_table')
            crs.execute('CREATE TABLE data_exchange_table (decimal1 DECIMAL)')
            for _ in range(0, 50):
                rdm = random.random()
                crs.execute('INSERT INTO data_exchange_table VALUES {}'.format(rdm))
            crs.commit()

    def tearDown(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            crs = ecn.cursor()
            crs.execute('OPEN SCHEMA exasol_travis_python')
            crs.execute('DROP TABLE data_exchange_table')
            crs.commit()


class ODBCOnlyTest(TestCase):
    def test_connect_disconnect(self):
        ecn = exasol.connect(**self.odbc_kwargs)
        crs = ecn.cursor()
        crs.execute('select * from dual')
        rows = crs.fetchall()
        self.assertEqual(1, len(rows))
        self.assertIsNone(rows[0][0])
        crs.close()
        ecn.close()

    def test_connect_disconnect_with_withstatement(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            crs = ecn.cursor()
            crs.execute('select * from dual')
            rows = crs.fetchall()
            self.assertEqual(1, len(rows))
            self.assertIsNone(rows[0][0])
        self.assertRaises(pyodbc.ProgrammingError, ecn.close)

    def test_serverAddress(self):
        host = socket.gethostbyname(self.odbc_kwargs['EXAHOST'].split(':')[0])
        port = int(self.odbc_kwargs['EXAHOST'].split(':')[1])
        with exasol.connect(**self.odbc_kwargs) as con:
            self.assertEqual((host, port), con.serverAddress)


class CSVTest(TestCase):
    def test_readCSV_gets_all_rows(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            rows = ecn.readCSV('SELECT decimal1 FROM exasol_travis_python.data_exchange_table')
            self.assertEqual(50, len(rows))

    def test_readCSV_returns_a_list_of_lists(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            rows = ecn.readCSV('SELECT decimal1 FROM exasol_travis_python.data_exchange_table')
            self.assertIsInstance(rows, list)
            self.assertIsInstance(rows[0], list)

    def test_readCSV_gets_plausible_data(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            crs = ecn.cursor()
            crs.execute('SELECT sum(decimal1) FROM exasol_travis_python.data_exchange_table')
            sum_ = crs.fetchone()[0]
            rows = ecn.readCSV('SELECT decimal1 FROM exasol_travis_python.data_exchange_table')
        self.assertEqual(sum_,
                         reduce(operator.add, [Decimal(row[0]) for row in rows if len(row)]))

    def test_writeCSV_works(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            c = ecn.cursor()
            c.execute('OPEN SCHEMA exasol_travis_python')
            c.execute('DROP TABLE IF EXISTS T')
            c.execute('CREATE TABLE T (x INT, y INT)')
            ecn.writeCSV([[1, 2], [3, 4]], 'T')

            rows = c.execute('SELECT * FROM exasol_travis_python.t').fetchall()
            self.assertEqual(2, len(rows))
            row0 = [x for x in rows[0]]
            row1 = [x for x in rows[1]]
            expected = sorted([row0, row1])
            result = sorted([[Decimal(1), Decimal(2)], [Decimal(3), Decimal(4)]])
            c.execute('DROP TABLE exasol_travis_python.t')
            self.assertEqual(expected, result)


class PandasTest(TestCase):
    def test_readPandas_gets_all_rows(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            rows = ecn.readPandas('SELECT decimal1 FROM exasol_travis_python.data_exchange_table')
            self.assertEqual(50, len(rows))

    def test_readPandas_returns_a_dataframe(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            rows = ecn.readPandas('SELECT decimal1 FROM exasol_travis_python.data_exchange_table')
            self.assertIsInstance(rows, pandas.DataFrame)

    def test_readPandas_gets_plausible_data(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            crs = ecn.cursor()
            crs.execute('SELECT sum(decimal1) FROM exasol_travis_python.data_exchange_table')
            sum_ = crs.fetchone()[0]
            rows = ecn.readPandas('SELECT decimal1 FROM exasol_travis_python.data_exchange_table')
        self.assertAlmostEqual(float(sum_), float(rows.sum()))

    def test_writePandas_works(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            c = ecn.cursor()
            c.execute('OPEN SCHEMA exasol_travis_python')
            c.execute('DROP TABLE IF EXISTS exasol_travis_python.t')
            c.execute('CREATE TABLE T (x INT, y VARCHAR(10))')

            # numpy arrays are transposed:
            data = pandas.DataFrame({1: [1, 2], 2: ["a", "b"]})
            ecn.writePandas(data, 'T')

            rows = c.execute('SELECT * FROM exasol_travis_python.t').fetchall()
            self.assertEqual(2, len(rows))
            row0 = [x for x in rows[0]]
            row1 = [x for x in rows[1]]
            expected = sorted([row0, row1])
            result = sorted([[Decimal(1), "a"], [Decimal(2), "b"]])
            c.execute('DROP TABLE exasol_travis_python.t')
            self.assertEqual(expected, result)


class DefaultsTest(TestCase):
    def test_readData_defaults_to_pandas(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            rows = ecn.readData('SELECT * FROM dual')
            self.assertIsInstance(rows, pandas.DataFrame, rows.__class__)

    def test_readData_set_default_with_connect(self):
        with exasol.connect(useCSV=True, **self.odbc_kwargs) as ecn:
            rows = ecn.readData('SELECT * FROM dual')
            self.assertIsInstance(rows, list, rows.__class__)

    def test_readData_overwrite_default(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            rows = ecn.readData('SELECT * FROM dual',
                                readCallback=exasol.csvReadCallback)
            self.assertIsInstance(rows, list, rows.__class__)

    def test_writeData_defaults_to_pandas(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            c = ecn.cursor()
            c.execute('OPEN SCHEMA exasol_travis_python')
            c.execute('DROP TABLE IF EXISTS T')
            c.execute('CREATE TABLE T (x INT, y INT)')

            with self.assertRaises(TypeError):
                ecn.writeData([[1, 2], [3, 4]], 'T')
            c.execute('DROP TABLE exasol_travis_python.t')

    def test_writeData_set_default_with_connect(self):
        with exasol.connect(useCSV=True, **self.odbc_kwargs) as ecn:
            c = ecn.cursor()
            c.execute('OPEN SCHEMA exasol_travis_python')
            c.execute('DROP TABLE IF EXISTS T')
            c.execute('CREATE TABLE T (x INT, y INT)')

            ecn.writeData([[1, 2], [3, 4]], 'T')
            c.execute('DROP TABLE T')

    def test_writeData_overwrite_default(self):
        with exasol.connect(**self.odbc_kwargs) as ecn:
            c = ecn.cursor()
            c.execute('OPEN SCHEMA exasol_travis_python')
            c.execute('DROP TABLE IF EXISTS T')
            c.execute('CREATE TABLE T (x INT, y INT)')

            ecn.writeData([[1, 2], [3, 4]], 'T',
                          writeCallback=exasol.csvWriteCallback)
            c.execute('DROP TABLE T')


if __name__ == '__main__':
    unittest.main(verbosity=2)

# vim: ts=4:sts=4:sw=4:et:fdm=indent
