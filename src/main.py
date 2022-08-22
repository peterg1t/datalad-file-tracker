from importlib.resources import path
import subprocess
import streamlit as st
import pandas as pd
import numpy as np
import datalad.api as dl
from datalad_metalad.extractors import runprov
from datalad_metalad import extract
from datalad_metalad.extract import Extract
from datalad_metalad.aggregate import Aggregate
import argparse
from os import curdir

st.write("""
#this is a text
""")

dataset = "/Users/pemartin/Scripts/datalad-test/Datalad-101"
dataset_in = "/Users/pemartin/Scripts/datalad-test/Datalad-101/inputs/I1"
dataset_out = "/Users/pemartin/Scripts/datalad-test/Datalad-101/outputs/O1"
file_dataset = "/Users/pemartin/Scripts/datalad-test/Datalad-101/inputs/I1/1280px-Philips_PM5544.svg.png"

#---------------------------BEGIN----------------------------
# ext = Extract()
# meta = ext(extractorname='image', path= file_dataset, dataset= dataset_in)

# or

# dl.meta_extract(extractorname='image', path= file_dataset, dataset= dataset_in)
# dl.meta_aggregate(path=dataset_out)
#----------------------------END-----------------------------


# outlogs=[]
# errlogs=[]
# run_command = f"datalad meta-extract -d {dataset_in} image 1280px-Philips_PM5544.svg.png|jq"
# run_command_output = subprocess.run(run_command, shell=True, capture_output=True, text=True, check=False)
# outlog = run_command_output.stdout.split('\n')
# errlog = run_command_output.stderr.split('\n')
# outlog.pop() # drop the empty last element
# errlog.pop() # drop the empty last element
# outlogs.append(outlog)
# errlogs.append(errlog)
# print(outlogs)
# print(errlogs)

# outlogs=[]
# errlogs=[]
# run_command = f"cd {dataset_out} && git log"
# run_command_output = subprocess.run(run_command, shell=True, capture_output=True, text=True, check=False)
# outlog = run_command_output.stdout.split('\n')
# errlog = run_command_output.stderr.split('\n')
# outlog.pop() # drop the empty last element
# errlog.pop() # drop the empty last element
# outlogs.append(outlog)
# errlogs.append(errlog)
# print(outlogs)
# print(errlogs)


class RunCmd:
    def __init__(self):
        self.cmd = ''
    
    def method1():
        print('nothing here yet')



def git_log_parse(dir):
    print('here')



parser = argparse.ArgumentParser() #pylint: disable = invalid-name
parser.add_argument('dirname', nargs='?', default=curdir, help='Path to the DataLad subdataset')
args = parser.parse_args() #pylint: disable = invalid-name

if args.dirname:
    dirname = args.dirname  #pylint: disable = invalid-name
    git_log_parse(dirname)