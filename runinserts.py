import sys

from DB.inserts import main

basedir = '/home/james/Documents/andersen_lab/bird_flu_db/test_data'
if len(sys.argv) >= 2:
    basedir = sys.argv[1]

main(basedir)
