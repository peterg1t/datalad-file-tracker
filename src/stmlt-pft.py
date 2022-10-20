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
import itertools
from pathlib import Path
import glob 


st.write("""
Welcome to file provenance tracker!
""")






class FileNote:
    def __init__(self, dataset, filename, relative, author, date, commit, summary, message):
        self.filename = filename
        self.dataset = dataset 
        self.author = author
        self.date = date
        self.relative = relative # child or parent of the file
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
        self._graph=None

    def plot_one_direction(self, trackline):
        if self._option == 'Process':
            # this section create the nodes of the graph
            for index, item in enumerate(trackline):
                if index == 0:
                    self._graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}", color="green")
                else:
                    self._graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}",color="red")
            
            #this section create the links on the graph between a file and its parents
            for index, item in enumerate(trackline[:-1]):
                for rel in item.relative:
                    for j in range(index, len(trackline)):
                        if rel == trackline[j].filename:
                            if self._mode == 'Reverse':
                                self._graph.edge(trackline[j].commit, item.commit)
                            elif self._mode == 'Forward':
                                self._graph.edge(item.commit, trackline[j].commit)

        elif self._option == 'Simple':
            # This section creates the nodes and links between the files (nodes of the tree)
            for index, item in enumerate(trackline):
                if index == 0:
                    self._graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}",color="green")
                else:
                    self._graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}",color="red")
                for rel in item.relative:
                    self._graph.node(rel, f"{rel}")
                    if self._mode == 'Reverse':
                        self._graph.edge(rel,item.filename)
                    elif self._mode == 'Forward':
                        self._graph.edge(item.filename,rel)

        now = datetime.now()
        time_stamp = datetime.timestamp(now)
        self._graph.render(f"/tmp/{time_stamp}",format='png')
        return st.graphviz_chart(self._graph,use_container_width=True)


    def plot_two_direction(self, trackline):
        if self._option == 'Process':
            # this section create the nodes of the graph
            for index, item in enumerate(trackline):
                if index == 0:
                    self._graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}", color="green")
                else:
                    self._graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}",color="red")
            
            #this section create the links on the graph between a file and its parents
            for index, item in enumerate(trackline[:-1]):
                for rel in item.relative:
                    for j in range(index, len(trackline)):
                        if rel == trackline[j].filename:
                            if self._mode == 'Reverse':
                                self._graph.edge(trackline[j].commit, item.commit)
                            elif self._mode == 'Forward':
                                self._graph.edge(item.commit, trackline[j].commit)

        elif self._option == 'Simple':
            # This section creates the nodes and links between the files (nodes of the tree)
            for index, item in enumerate(itertools.islice(trackline,len(trackline)//2)):
                self._graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}")
                for rel in item.relative:
                    self._graph.node(rel, f"{rel}")
                    print(f"{rel}")
                    self._graph.edge(item.filename,rel)

            for index, item in enumerate(itertools.islice(trackline,len(trackline)//2, len(trackline), 1)):
                self._graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}")
                for rel in item.relative:
                    self._graph.node(rel, f"{rel}")
                    print(f"{rel}")
                    self._graph.edge(item.filename,rel)

        now = datetime.now()
        time_stamp = datetime.timestamp(now)
        self._graph.render(f"/tmp/{time_stamp}",format='png')
        return st.graphviz_chart(self._graph,use_container_width=True)



    
    def plot_notes(self):
        """ This function will generate the graphviz plots

        Returns:
            _type_: _description_
        """
        self._graph = graphviz.Digraph(node_attr={'shape': 'record'})
        self._graph.attr(rankdir='TB')  

        if self._mode == 'Bidirectional':
            self.plot_two_direction(self._trackline)
        else:
            self.plot_one_direction(self._trackline)
            

    








class FileTrack:
    def __init__(self, file, s_option, l_option):
        self.file = file #file to build the file tree in the dataset from its first occurernce
        
        print('inital_get_git_root')
        self.dataset = self._get_git_root_initial(file)
        self.sds = self._get_superdataset()        

        self.search_option = s_option
        self.level_limit = l_option
        self.level = 1
        self.trackline = []

    def _add_note(self, note):
        """ This function will append a note to the trackline

        Args:
            note (object): The instance of the object to append to the trackline
        """
        self.trackline.append(note)
    
    def _delete_note(self, note):
        """ This function will delete a note

        Args:
            note (object): The object to remove from the trackline
        """
        self.trackline.pop(note)


    # def _iter_scan(self, cm_list):
    #     """! This function will iteratively scan for the parent of a file object

    #     Args:
    #         cm_list (str): A list of DATALAD RUNCMD string commits
    #     """
    #     if self.search_option == 'Reverse':
    #         order = ('outputs','inputs')
    #     elif self.search_option == 'Forward':
    #         order = ('inputs','outputs')

    #     for item in cm_list:
    #         dict_object = ast.literal_eval(re.search('(?=\{)(.|\n)*?(?<=\}\n)', item.message).group(0))     
    #         if dict_object[order[0]]:
    #             basename_input_file = os.path.basename(os.path.abspath(self.file))
    #             basename_dataset_files = os.path.basename(os.path.abspath(os.path.join(self.dataset,dict_object[order[0]][0])))
    #             if basename_dataset_files == basename_input_file: #found the file wich in the first run is the input
    #                 files = dict_object[order[1]]
    #                 instanceNote = FileNote(self.dataset, self.file, files, item.author, item.committed_date, \
    #                     item.hexsha, item.summary, item.message)
    #                 self._add_note(instanceNote)
    #                 for f in files:
    #                     self.file = os.path.abspath(os.path.join(self.superdataset,f))
    #                     self.dataset = self._get_git_root(self.file)
    #                     self._iter_scan(cm_list)

        
    def _iter_scan_mod(self, cm_list, input_file):
        """! This function will iteratively scan for the parent of a file object and update the file track

        Args:
            cm_list (str): A list of DATALAD RUNCMD string commits
        """
        print('scan_get_git_root', input_file)
        dataset = self._get_git_root(os.path.join(self.sds.path,input_file))

        if self.search_option == 'Reverse':
            order = ('outputs','inputs')
        elif self.search_option == 'Forward':
            order = ('inputs','outputs')
        
        files = self._iter_scan_kernel(cm_list, order, dataset, input_file)
        # for file in files:
        #     self._iter_scan_mod(cm_list,file)
        
        return files



    def _iter_scan_kernel(self, cm_list, order, dataset, input_file):
        for item in cm_list:
            dict_object = ast.literal_eval(re.search('(?=\{)(.|\n)*?(?<=\}\n)', item.message).group(0))
            if dict_object[order[0]]:
                basename_input_file = os.path.basename(os.path.abspath(input_file))
                basename_dataset_files = os.path.basename(os.path.abspath(os.path.join(dataset,dict_object[order[0]][0])))
                # print(basename_input_file, basename_dataset_files)
                if basename_dataset_files == basename_input_file: #found the file wich in the first run is the input
                    files = dict_object[order[1]]
                    # print('files',files,dict_object)
                    instanceNote = FileNote(dataset, input_file, files, item.author, item.committed_date, \
                        item.hexsha, item.summary, item.message)
                    self._add_note(instanceNote)
                    return files

                
    def _get_git_root_initial(self, path_initial_file):
        git_repo = git.Repo(path_initial_file, search_parent_directories=True)
        git_root = git_repo.git.rev_parse("--show-toplevel")
        
        return git_root
                        
        
    def _get_git_root(self,path_file):
        real_path = glob.glob(f"{self.sds.path}/**/{os.path.basename(path_file)}",recursive=True)[0]
        git_repo = git.Repo(real_path, search_parent_directories=True)
        # print('git_repo',git_repo)
        git_root = git_repo.git.rev_parse("--show-toplevel")
        
        return git_root

    def _get_superdataset(self):
        ds = dl.Dataset(self.dataset)
        sds = ds.get_superdataset()
  
        return sds





    def _get_commit_list(self, commits, run_cmd_commits): 
            for item in commits:
                if 'DATALAD RUNCMD' in item.message:
                    run_cmd_commits.append(item)
            # return run_cmd_commits

    def search_level(self, commits):
        relatives = self._iter_scan_mod(commits, self.file)
        while self.level < self.level_limit:
            rp = relatives
            # print('relatives',rp)
            if rp is not None:
                self.level = self.level + 1 
                for relative in rp:
                    relatives = self._iter_scan_mod(commits, relative)
            else:
                self.level = self.level_limit



    def search(self):
        all_commits=[]
        dataset_list = []
        """! This function will return all the instances of a file search
        repo is the git repo corresponding to a dataset
        """
        # print('search_get_git_root')
        super_ds = self._get_superdataset()
        dataset_list.append(super_ds.path)
        subdatasets = super_ds.subdatasets()
        
        for subdataset in subdatasets:
            dataset_list.append(subdataset['path'])

        #now that we have all the datasets in one list lets find all the commits that were generated by datalad run
        for subdataset_path in dataset_list:        
            repo = git.Repo(subdataset_path)
            commits = list(repo.iter_commits('master'))
            self._get_commit_list(commits, all_commits)

        if self.search_option == 'Bidirectional':
            self.search_option = 'Reverse'
            self.search_level(all_commits)
            self.trackline.reverse()
            print(self.trackline)
            self.search_option = 'Forward'
            self.search_level(all_commits)
            print(self.trackline)

        else:
            self.search_level(all_commits)
            print(self.trackline)
            for item in self.trackline:
                print('filename item',item.filename)
            

        

        

  





def git_log_parse(filename, s_option, g_option, l_option):
    """! This function will generate the graphs and objects to represent the filetrack

    Args:
        filename (str): An absolute path to the filename
        s_option (str): A search option (Reverse/Forward)
        g_option (str): A graph display mode (Process/Simple)
    """
    file_notes = FileTrack(filename, s_option, l_option) #given a filename and a search option we decide to search for all parents or all childs to fill the file track list
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






if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--filepath", help="path to file")
    parser.add_argument("-s", "--search_mode", help="mode to search (Reverse/Forward)", choices=['Reverse','Forward'])
    parser.add_argument("-d", "--display_mode", help="display type (Process/Simple)", choices=['Process', 'Simple'])
    
    args = parser.parse_args()  # pylint: disable = invalid-name

    if args.filepath and args.search_mode and args.display_mode:
        flnm = args.filepath
        search_option = args.search_mode
        plot_option = args.display_mode
    else: 
        print("Not all command line arguments were used as input, results might be wrong")
        flnm = st.text_input('Input the file to track')
        search_option = st.selectbox('Search mode', ['Reverse','Forward', 'Bidirectional'])
        plot_option = st.selectbox('Display mode', ['Simple','Process'])
        plot_levels = st.slider('Levels', 1, 10, 1)

    # Sreamlit UI implementation
    

    if flnm:
        git_log_parse(flnm,search_option,plot_option, plot_levels)