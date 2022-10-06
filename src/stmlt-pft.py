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
        self._trackline = trackline
        self._mode = mode
        self._option = option
        
    
    def plot_notes(self):
        """ This function will generate the graphviz plots

        Returns:
            _type_: _description_
        """
        graph = graphviz.Digraph(node_attr={'shape': 'record'})
        graph.attr(rankdir='TB')  

        if self._option == 'Process':
        # this section create the nodes of the graph
            for index, item in enumerate(self._trackline):
                graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}")
            
        
            #this section create the links on the graph between a file and its parents
            for index, item in enumerate(self._trackline[:-1]):
                for rel in item.relative:
                    for j in range(index, len(self._trackline)):
                        if rel == self._trackline[j].filename:
                            if self._mode == 'Reverse':
                                graph.edge(self._trackline[j].commit, item.commit)
                            elif self._mode == 'Forward':
                                graph.edge(item.commit, self._trackline[j].commit)
        
        elif self._option == 'Simple':
            # This section creates the nodes and links between the files (nodes of the tree)
            for index, item in enumerate(self._trackline):
                graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}")
                for rel in item.relative:
                    graph.node(rel, f"{rel}")
                    if self._mode == 'Reverse':
                        graph.edge(rel,item.filename)
                    elif self._mode == 'Forward':
                        graph.edge(item.filename,rel)
                
        return st.graphviz_chart(graph,use_container_width=True)

    








class FileTrack:
    def __init__(self, file, s_option):
        self.file = file #file to build the file tree in the dataset from its first occurernce
        
        self.dataset = self.get_git_root(file)
        ds = dl.Dataset(self.dataset)
        sds = ds.get_superdataset()
        self.superdataset = sds.path

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
                        self.file = os.path.abspath(os.path.join(self.superdataset,pf))
                        self.dataset = self.get_git_root(self.file)
                        self.iter_scan_pt(cm_list)

    def iter_scan_ch(self, cm_list):
        """! This function will iteratively scan for the parent of a file object

        Args:
            cm_list (str): A list of DATALAD RUNCMD string commits
        """

        for item in cm_list:
            dict_object = ast.literal_eval(re.search('(?=\{)(.|\n)*?(?<=\}\n)', item.message).group(0))

            if dict_object['inputs']:
                basename_input_file = os.path.basename(os.path.abspath(self.file))
                basename_dataset_files = os.path.basename(os.path.abspath(os.path.join(self.dataset,dict_object['inputs'][0])))

                if basename_dataset_files == basename_input_file:
                    child_files = dict_object['outputs']
                    instanceNote = FileNote(self.dataset, self.file, child_files, item.author, item.committed_date, \
                        item.hexsha, item.summary, item.message)
                    self.add_note(instanceNote)
                    for cf in child_files:
                        self.file = os.path.abspath(os.path.join(self.superdataset,cf))
                        self.dataset = self.get_git_root(self.file)
                        self.iter_scan_ch(cm_list)
    

    def get_git_root(self,path_ff):
        git_repo = git.Repo(path_ff, search_parent_directories=True)
        git_root = git_repo.git.rev_parse("--show-toplevel")
        return git_root

    def get_commit_list(self, commits, run_cmd_commits): 
            for item in commits:
                if 'DATALAD RUNCMD' in item.message:
                    run_cmd_commits.append(item)
            # return run_cmd_commits


    def search(self):
        all_commits=[]
        dataset_list = []
        """! This function will return all the instances of a file search
        repo is the git repo corresponding to a dataset
        """
        repo_str = self.get_git_root(self.file)
        ds = dl.Dataset(repo_str)
        sds = ds.get_superdataset()
        dataset_list.append(sds.path)
        subdatasets = sds.subdatasets()
        
        for subdataset in subdatasets:
            dataset_list.append(subdataset['path'])

        #now that we have all the datasets in one list lets find all the commits that were generated by datalad run
        for subdataset_path in dataset_list:        
            repo = git.Repo(subdataset_path)
            commits = list(repo.iter_commits('master'))
            self.get_commit_list(commits, all_commits)


                  

        if self.search_option == 'Reverse':
            print('scanning_reverse')
            self.iter_scan_pt(all_commits)
        elif self.search_option == 'Forward':
            print('scanning_forward')
            self.iter_scan_ch(all_commits)

  


    


def git_log_parse(filename, s_option, g_option):
    """! This function will generate the graphs and objects to represent the filetrack

    Args:
        filename (str): An absolute path to the filename
        s_option (str): A search option (Reverse/Forward)
        g_option (str): A graph display mode (Process/Simple)
    """
    file_notes = FileTrack(filename, s_option)
    file_notes.search()

    #Once the trackline is calculated we use it to generate a graph in graphviz
    if not file_notes.trackline and s_option == 'Reverse':
        st.info('There is no trackline for this specific file, there are no parents to this file')
    elif not file_notes.trackline and s_option == 'Forward':
        st.info('There is no trackline for this specific file, there are no childs to this file')
    else:
        st.info('Plotting results')
        graph = PlotNotes(file_notes.trackline, s_option, g_option)
        graph.plot_notes()



# Sreamlit UI implementation
flnm = st.text_input('Input the file to track')
search_option = st.selectbox('Search mode', ['Reverse','Forward'])
plot_option = st.selectbox('Display mode', ['Simple','Process'])

if flnm:
    git_log_parse(flnm,search_option,plot_option)

