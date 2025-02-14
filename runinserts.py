import sys

from DB.inserts import main

basedir = '/home/james/documents/andersen_lab'
if len(sys.argv) >= 1:
    basedir = sys.argv[0]
main(basedir)
