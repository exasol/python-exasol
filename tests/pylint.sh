#!/bin/bash


# die in case of errors
set -e

if [ $# -eq 2 ]; then
    RTDIR="$1"
    TBDIR="$2"
else
    echo "Usage: $0 runtime-dir toolbox_py-dir" >&2
    exit 2
fi

if `ls -l /proc/$$/fd/1 | grep -q '/dev/pts'`; then
    FORMAT=colorized
else
    FORMAT=parseable
fi

srcdir=/x/pkg/ftp.logilab.org/pub
python=$RTDIR/bin/python


TMPDIR=`mktemp -d -t test_toolbox_pylint-XXXXXXXXXX`
installdir="$TMPDIR/py/lib/python"
mkdir -p "$installdir"
export PYTHONPATH="$installdir"
PYLINT="$TMPDIR/py/bin/pylint"

(
    set -e
    cd  $TMPDIR
    for x in $srcdir/pylint/pylint-0.23.0.tar.gz $srcdir/astng/logilab-astng-0.21.1.tar.gz $srcdir/common/logilab-common-0.54.0.tar.gz /x/pkg/pypi.python.org/packages/source/u/unittest2/unittest2-0.5.1.tar.gz; do
        tar -xzf $x;
    done
    ls
    cd $TMPDIR/unittest2-*
    $python setup.py install --home=$TMPDIR/py
    cd $TMPDIR/logilab-common-*
    $python setup.py install --home=$TMPDIR/py
    cd $TMPDIR/logilab-astng-*
    $python setup.py install --home=$TMPDIR/py
    cd $TMPDIR/pylint-*
    $python setup.py install --home=$TMPDIR/py
) >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "pylint installation failed" >&2
    rm -rf "$TMPDIR"
    exit 1
fi
(
    cd $TBDIR
    tree
    export PYLINTHOME=$TMPDIR
    set +e
    $PYLINT --rcfile=/dev/null \
        --output-format=$FORMAT \
        --reports=n \
        --disable=R,C \
        --include-ids=y \
        --disable=W0102 \
        --disable=W0201 \
        --disable=W0402,W0404 \
        --disable=W0602,W0622 \
        --disable=W0702,W0703 \
        --disable=E0213 \
        --disable=E0702 \
        `ls *.py`
)
rc=$?
echo "pylint return code: $rc" >&2
rm -rf "$TMPDIR"
exit $rc

# vim: ts=4:sts=4:sw=4:et:fdm=indent
