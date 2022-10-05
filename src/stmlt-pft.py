import cmd
import os
from os import curdir
from tkinter.messagebox import NO

import git
import re
import ast
from fileinput import filename
from importlib.resources import path
from importlib_metadata import method_cache
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






class FileNote:
    def __init__(self, dataset, filename, relative, author, date, commit, summary, message):
        self.filename = filename
        self.dataset = dataset #dataset where the data belongs
        self.author = author
        self.date = date
        self.relative = relative
        self.commit = commit #commit that created the file
        self.summary = summary
        self.message = message






class PlotNotes:
    """ This is a builder class for the graphs
    """
    def __init__(self, trackline, mode, option):
        self.trackline = trackline
        self.option = option
    
        if option == 'Process' and mode == 'Reverse':
            self.plot_notes_rev()
        elif option == 'Process' and mode == 'Forward':
            self.plot_notes_fwd()
        elif option == 'Simple' and mode == 'Reverse':
            self.plot_notes_simple_rev()
        elif option == 'Simple' and mode == 'Forward':
            self.plot_notes_simple_fwd()







    def plot_notes_rev(self):
        """ This function will plot the notes to a graphviz
        """
        graph = graphviz.Digraph(node_attr={'shape': 'record'})
        graph.attr(rankdir='TB')  

        # this section create the nodes of the graph
        for index, item in enumerate(self.trackline):
            graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}")
            print(f"message={item.message}")
            
        #this section create the links on the graph between a file and its parents
        for index, item in enumerate(self.trackline[:-1]):
            for rel in item.relative:
                for j in range(index, len(self.trackline)):
                    if rel == self.trackline[j].filename:
                        graph.edge(self.trackline[j].commit, item.commit)
                
        return st.graphviz_chart(graph,use_container_width=True)



    def plot_notes_fwd(self):
        """ This function will plot the notes to a graphviz
        """
        graph = graphviz.Digraph(node_attr={'shape': 'record'})
        graph.attr(rankdir='TB')  

        # this section create the nodes of the graph
        for index, item in enumerate(self.trackline):
            graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}")
            print(f"message={item.message}")
            
        #this section create the links on the graph between a file and its parents
        for index, item in enumerate(self.trackline[:-1]):
            for rel in item.relative:
                for j in range(index, len(self.trackline)):
                    if rel == self.trackline[j].filename:
                        graph.edge(self.trackline[j].commit, item.commit)
                
        return st.graphviz_chart(graph,use_container_width=True)




    def plot_notes_simple_rev(self):
        """ This function will append the notes to the graphviz
        """
        graph = graphviz.Digraph(node_attr={'shape': 'record'})
        graph.attr(rankdir='TB')  
 
        # This section creates the nodes and links between the files (nodes of the tree)
        for index, item in enumerate(self.trackline):
            graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}")
            for rel in item.relative:
                graph.node(rel, f"{rel}")
                graph.edge(rel,item.filename)
                
        return st.graphviz_chart(graph,use_container_width=True)








    def plot_notes_simple_fwd(self):
        """ This function will append the notes to the graphviz
        """
        graph = graphviz.Digraph(node_attr={'shape': 'record'})
        graph.attr(rankdir='TB')  
 
        # This section creates the nodes and links between the files (nodes of the tree)
        for index, item in reversed(list(enumerate(self.trackline))):
            graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}")
            for rel in item.relative:
                graph.node(rel, f"{rel}")
                graph.edge(item.filename,rel)
                
        return st.graphviz_chart(graph,use_container_width=True)






class FileTrack:
    def __init__(self, file, s_option):
        self.file = file #file to build the file tree in the dataset from its first occurernce
        self.dataset = self.get_git_root(file)
        self.search_option = s_option
        self.trackline = []

    def add_note(self, note):
        """ This function will append a note to the trackline

        Args:
            note (object): The instance of the object to append to the trackline
        """
        self.trackline.append(note)
    
    def delete_note(self, note):
        """ This function will delete a note

        Args:
            note (object): The object to remove from the trackline
        """
        self.trackline.pop(note)

        
    def iter_scan_pt(self, cm_list):
        """! This function will iteratively scan for the parent of a file object

        Args:
            cm_list (str): A list of DATALAD RUNCMD string commits
        """
        for item in cm_list:
            dict_object = ast.literal_eval(re.search('(?=\{)(.|\n)*?(?<=\}\n)', item.message).group(0))        
            if dict_object['outputs']:
                basename_input_file = os.path.basename(os.path.abspath(self.file))
                basename_dataset_files = os.path.basename(os.path.abspath(os.path.join(self.dataset,dict_object['outputs'][0])))
                if basename_dataset_files == basename_input_file:
                    parent_files = dict_object['inputs']
                    instanceNote = FileNote(self.dataset, self.file, parent_files, item.author, item.committed_date, \
                        item.hexsha, item.summary, item.message)
                    self.add_note(instanceNote)
                    for pf in parent_files:
                        self.file = os.path.abspath(os.path.join(self.dataset,pf))
                        path_repo = self.get_git_root(self.file)
                        if path_repo != self.dataset:
                            self.dataset = path_repo
                            self.search()
                        self.iter_scan_pt(cm_list)

    def iter_scan_ch(self, cm_list):
        """! This function will iteratively scan for the parent of a file object

        Args:
            cm_list (str): A list of DATALAD RUNCMD string commits
        """

        for item in cm_list:
            dict_object = ast.literal_eval(re.search('(?=\{)(.|\n)*?(?<=\}\n)', item.message).group(0))
            # print(dict_object['inputs'])
            # print(dict_object['outputs'])

            if dict_object['inputs']:
                print('do', dict_object, dict_object['inputs'])
                basename_input_file = os.path.basename(os.path.abspath(self.file))
                basename_dataset_files = os.path.basename(os.path.abspath(os.path.join(self.dataset,dict_object['inputs'][0])))
                print('ifile', basename_input_file)
                print('ds_files', basename_dataset_files)
                if basename_dataset_files == basename_input_file:
                    child_files = dict_object['outputs']
                    print('cfiles',child_files)
                    instanceNote = FileNote(self.dataset, self.file, child_files, item.author, item.committed_date, \
                        item.hexsha, item.summary, item.message)
                    self.add_note(instanceNote)
                    for cf in child_files:
                        print('child',cf)
                        self.file = os.path.abspath(os.path.join(self.dataset,cf))
                        path_repo = self.get_git_root(self.file)
                        if path_repo != self.dataset:
                            self.dataset = path_repo
                            self.search()
                        self.iter_scan_ch(cm_list)
    

    def get_git_root(self,path_ff):
        git_repo = git.Repo(path_ff, search_parent_directories=True)
        git_root = git_repo.git.rev_parse("--show-toplevel")
        return git_root

    

    def search(self):
        def get_commit_list(commits):
            run_cmd_commits=[]
            for item in commits:
                if 'DATALAD RUNCMD' in item.message:
                    run_cmd_commits.append(item)
            return run_cmd_commits


        """! This function will return all the instances of a file search
        repo is the git repo corresponding to a dataset
        """
        repo_str = self.get_git_root(self.file)
        repo = git.Repo(repo_str)
        commits = list(repo.iter_commits('master'))
        rcc = get_commit_list(commits)
                
        # Since a file RUN_COMMAND might be recorded in a superdataset
        if not rcc:
            ds = dl.Dataset(repo_str)
            sds = ds.get_superdataset().path
            repo = git.Repo(sds)
            commits = list(repo.iter_commits('master'))
            rcc = get_commit_list(commits)


        if self.search_option == 'Reverse':
            print('scanning_reverse')
            self.iter_scan_pt(rcc)
        elif self.search_option == 'Forward':
            print('scanning_forward')
            self.iter_scan_ch(rcc)

  


    


def git_log_parse(filename, s_option, g_option):
    file_tree = FileTrack(filename, s_option)
    file_tree.search()
    st.info('Plotting results')

    #Once the trackline is calculated we use it to generate a graph in graphviz
    graph = PlotNotes(file_tree.trackline, s_option, g_option)



# Sreamlit UI implementation
# dirname = st.text_input('Input the dataset')
flnm = st.text_input('Input the file to track')
search_option = st.selectbox('Search mode', ['Reverse','Forward'])
plot_option = st.selectbox('Display mode', ['Simple','Process'])

if flnm:
    git_log_parse(flnm,search_option,plot_option)

