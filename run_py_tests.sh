#!/bin/bash

ErrorHandler () {
  # Bail if a script fails to help isolate the error (since it's likely subsequent
  # scripts will fail too)
  echo -e "\n!!!!! ERROR OCCURRED, BAILING !!!!!\n"
  exit
}

trap ErrorHandler ERR

echo -e "\n***** PYTHON UNIT TESTS *****\n"
./run_py_unit_tests.sh
echo -e "\n***** PYTHON INTEGRATION TESTS *****\n"
./run_py_int_tests.sh
