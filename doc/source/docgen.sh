#!/bin/bash
export PROJECT_DIR=${PWD%/*/*}

# create documentation
doxygen Doxyfile
