#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Writes odbc.ini and odbcinst.ini for tests purposes into home directory. With customized path to odbc install dir"
  echo " "
  echo "Syntax:"
  echo " "
  echo "  $(basename $0) <odbc-install-dir>"
  exit 1
fi

ODBC_INSTALL_DIR=$1
echo "Write odbc ini files for odbs install dir $ODBC_INSTALL_DIR"
echo " "

cat > $HOME/odbc.ini <<EOL
[DEFAULT]
DRIVER=EXAODBC_TEST

[ODBC Data Sources]
exasolution-uo2214lv1_64 = unixODBC 2.2.14 or 2.3.0, libversion 1 (64bit)

[exasolution-uo2214lv1_64]
DRIVER = $ODBC_INSTALL_DIR/lib/linux/x86_64/libexaodbc-uo2214lv1.so

[exasolution-uo2214lv1_64-debug]
DRIVER = $ODBC_INSTALL_DIR/lib/linux/x86_64/libexaodbc-uo2214lv1.so
EXALOGFILE = $ODBC_INSTALL_DIR/exaodbc.log
LOGMODE = verbose

[EXAODBC_TEST]
DRIVER = EXAODBC

EOL

cat > $HOME/odbcinst.ini <<EOL
[ODBC]

[ODBC Drivers]
EXAODBC = Installed

[EXAODBC]
Driver = $ODBC_INSTALL_DIR/lib/linux/x86_64/libexaodbc-uo2214lv1.so
EOL

echo "$HOME/odbcinst.ini:"
cat $HOME/odbcinst.ini
echo " "
echo "$HOME/odbc.ini: "
cat $HOME/odbc.ini

