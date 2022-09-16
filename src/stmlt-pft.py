import os
from os import curdir
from tkinter.messagebox import NO

import git
import re
import ast
from fileinput import filename
from importlib.resources import path
import streamlit as st
import graphviz as graphviz
from datetime import datetime

import datalad.api as dl
from datalad_metalad.extractors import runprov
from datalad_metalad import extract
from datalad_metalad.extract import Extract
from datalad_metalad.aggregate import Aggregate
import argparse


st.write("""
Welcome to file provenance tracker!
""")

# dataset = "/Users/pemartin/Scripts/datalad-test/Datalad-101"
# dataset_in = "/Users/pemartin/Scripts/datalad-test/Datalad-101/inputs/I1"
# dataset_out = "/Users/pemartin/Scripts/datalad-test/Datalad-101/outputs/O1"
# file_dataset = "/Users/pemartin/Scripts/datalad-test/Datalad-101/inputs/I1/1280px-Philips_PM5544.svg.png"

class FileNote:
    def __init__(self, dataset, filename, parent, author, date, commit, message):
        self.filename = filename
        self.dataset = dataset #dataset where the data belongs
        self.author = author
        self.date = date
        self.parent = parent
        self.child = None
        self.commit = commit #commit that created the file
        self.message = message






class FileTrack:
    def __init__(self, dataset, file):
        self.dataset = dataset #dataset where the data belongs
        self.file = file #file to build the file tree in the dataset from its first occurernce
        self.trackline = []



    #     return outlogs
    def iter_scan(self, cm_list):
        for item in cm_list:
            dict_object = ast.literal_eval(re.search('(?=\{)(.|\n)*?(?<=\}\n)', item.message).group(0))
        
            if dict_object['outputs']:
                basename_input_file = os.path.basename(os.path.abspath(self.file))
                basename_dataset_files = os.path.basename(os.path.abspath(os.path.join(self.dataset,dict_object['outputs'][0])))
                if basename_dataset_files == basename_input_file:
                    parent_files = dict_object['inputs'] 
                    instanceNote = FileNote(self.dataset, self.file, parent_files, item.author, item.committed_date, item.hexsha, item.message)
                    self.trackline.append(instanceNote)                    
                    for pf in parent_files:
                        self.file = os.path.abspath(os.path.join(self.dataset,pf))
                        self.iter_scan(cm_list)


    

    def search(self):
        """! This function will return all the instances of a file search
        """
        repo = git.Repo(self.dataset)
        commits = list(repo.iter_commits('master'))
        run_cmd_commits=[]
        for item in commits:
            if 'DATALAD RUNCMD' in item.message:
                run_cmd_commits.append(item)
                
        
        
        self.iter_scan(run_cmd_commits)
    


    def plot_notes(self):
        graph = graphviz.Digraph(node_attr={'shape': 'record'})
        graph.attr(rankdir='TB')  

        # graph.node('struct1', '<f0> left|<f1> middle|<f2> right')
        # graph.node('struct2', '<f0> one|<f1> two')
        # graph.node('struct3', r'hello\nworld |{ b |{c|<here> d|e}| f}| g | h')

        # graph.edges([('struct1:f1', 'struct2:f0'), ('struct1:f2', 'struct3:here')])

        for index, item in enumerate(self.trackline):
            graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.parent} }}")
            
        
        for index, item in enumerate(self.trackline[:-1]):
            for pt in item.parent:
                for j in range(index, len(self.trackline)):
                    if pt == self.trackline[j].filename:
                        graph.edge(self.trackline[j].commit, item.commit)
                
            
        
        st.graphviz_chart(graph,use_container_width=True)




    def plot_notes_simple(self):
        graph = graphviz.Digraph(node_attr={'shape': 'record'})
        graph.attr(rankdir='TB')  
        # graph.node('struct1', '<f0> left|<f1> middle|<f2> right')
        # graph.node('struct2', '<f0> one|<f1> two')
        # graph.node('struct3', r'hello\nworld |{ b |{c|<here> d|e}| f}| g | h')

        # graph.edges([('struct1:f1', 'struct2:f0'), ('struct1:f2', 'struct3:here')])

        for index, item in enumerate(self.trackline):
            graph.node(item.filename, f"{item.filename}")
            for pt in item.parent:
                graph.node(pt, f"{pt}")
                graph.edge(pt,item.filename)
        
        
                
        st.graphviz_chart(graph,use_container_width=True)




def git_log_parse(dirname,filename,g_option):
    file_tree = FileTrack(dirname, filename)
    file_tree.search()
    st.info('Plotting results')
    if g_option == 'Simple':
        file_tree.plot_notes_simple()
    elif g_option == 'Process':
        file_tree.plot_notes()
    



dirname = st.text_input('Input the dataset')
fl = st.text_input('Input the file to track')
plot_option = st.selectbox('Display mode', ['Simple','Process'])
if dirname and fl:
    git_log_parse(dirname,fl,plot_option)

# parser = argparse.ArgumentParser() #pylint: disable = invalid-name
# parser.add_argument('dataset', nargs='?', default=curdir, help='Path to the DataLad subdataset')
# parser.add_argument('-f', '--filename', help='Path to the DataLad file to track')
# # parser.add_argument('-f', '--filename', action='append', help='Path to the DataLad file to track')
# args = parser.parse_args() #pylint: disable = invalid-name

# if args.dataset and args.filename:
#     dirname = args.dataset  #pylint: disable = invalid-name
#     fl = args.filename
#     git_log_parse(dirname,fl)
