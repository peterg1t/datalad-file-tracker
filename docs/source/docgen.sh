#!/bin/bash
export PROJECT_DIR=${PWD%/*/*}

# create documention
doxygen Doxyfile
