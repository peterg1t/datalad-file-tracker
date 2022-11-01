import cmd
import os
from os import curdir
import profile
from tkinter.messagebox import NO

import git
import re
import ast
from fileinput import filename
from importlib.resources import path
from importlib_metadata import method_cache
from sqlalchemy import between
import streamlit as st



import datalad.api as dl
from datalad_metalad.extractors import runprov
from datalad_metalad import extract
from datalad_metalad.extract import Extract
from datalad_metalad.aggregate import Aggregate
import argparse

from pathlib import Path
import glob 

import utils

import matplotlib.pyplot as plt

import cProfile, pstats



profiler = cProfile.Profile()

st.write("""
Welcome to file provenance tracker!
""")




class FileTrack:
    """! Class filetrack to store the info regarding the notes (nodes)
    """
    def __init__(self, file, s_option, l_option):
        self.file = file #file to build the file tree in the dataset from its first occurernce
        
        self.dataset = self._get_git_root_initial(file)
        self.sds = self._get_superdataset()        

        self.search_option = s_option # search option forward or backward
        self.level_limit = l_option # the depth level to search in the tree

        self.trackline = [] # a track of notes
        self.queue=[] # a queue of notes
        self.queue_invar=[] # a queue that only gets appended
        self.queue_level=[] # a list of the levels of every note on the tree



    def _add_note(self, note):
        """ This function will append a note to the trackline

        Args:
            note (object): The instance of the object to append to the trackline
        """
        self.trackline.append(note)


    def _add_queue(self, filename):
        """ This function will append a node to the queue

        Args:
            filename (str): The file name to add to the queue
        """
        self.queue.append(filename)


    def _extend_queue(self, list_files):
        """ This function will extend the queue with the relatives of a certain note

        Args:
            list_files (list): A list to append to the queue
        """
        self.queue.extend(list_files)


    def _pop_queue(self):
        """This function will pop the first element of the queue
        """
        self.queue.pop(0)





    def _delete_note(self, note):
        """ This function will delete a note

        Args:
            note (object): The object to remove from the trackline
        """
        self.trackline.pop(note)




        
    def _iter_scan_mod(self, cm_list, input_file):
        """! This function will iteratively scan for the parent of a file object and update the file track

        Args:
            cm_list (str): A list of DATALAD RUNCMD string commits
        """
        dataset = self._get_git_root(os.path.join(self.sds.path,input_file))
        print('calling dataset')

        if self.search_option == 'Reverse':
            order = ('outputs','inputs')
        elif self.search_option == 'Forward':
            order = ('inputs','outputs')
        
        files = self._iter_scan_kernel(cm_list, order, dataset, input_file)
        

        
                
            

            


    def _iter_scan_kernel(self, cm_list, order, dataset, input_file) -> list:
        """! This function will return all the child or parent notes

        Args:
            cm_list (list): A list of commits
            order (list): The order to search the tree from "outputs" to "inputs" or viceversa
            dataset (str): A path to the dataset
            input_file (str): The name of the input file

        Returns:
            _type_: _description_
        """
        for item in cm_list:
            dict_object = ast.literal_eval(re.search('(?=\{)(.|\n)*?(?<=\}\n)', item.message).group(0))
            if dict_object[order[0]]:
                basename_input_file = os.path.basename(os.path.abspath(input_file))
                basename_dataset_files = os.path.basename(os.path.abspath(os.path.join(dataset,dict_object[order[0]][0])))
                if basename_dataset_files == basename_input_file: #found the file wich in the first run is the input
                    relative_notes = dict_object[order[1]]
                    
                    #get the full path of the relatives notes
                    relative_notes = [os.path.abspath(os.path.join(self._get_git_root(os.path.join(self.sds.path,f)),os.path.basename(f))) for f in relative_notes]
                    instanceNote = utils.FileNote(dataset, os.path.abspath(os.path.join(dataset,os.path.basename(input_file))), relative_notes, item.author, item.committed_date, \
                        item.hexsha, item.summary, item.message)
                    self._add_note(instanceNote)

                    # Add the relative notes to the queue and invariant queue
                    self._extend_queue(relative_notes)
                    self.queue_invar.extend(relative_notes)

                    #get the index of the input file in the invariant queue
                    self.index_file = self.queue_invar.index(input_file)
                    #get the previous index
                    self.index_previous = self.index_file - 1

                    #get the depth level from the queue level
                    self.depth_level = self.queue_level[self.index_file]
                    #get the previous depth level to compare
                    self.depth_compare = self.queue_level[self.index_previous]

                    #extend the queue level
                    self.queue_level.extend( len(relative_notes)*[self.depth_level+1] )

                    #pop  the queue
                    self._pop_queue()

                    if self.depth_compare == self.level_limit and self.depth_level > self.level_limit:
                        self.trackline.pop()
                    
                    else:
                        if self.queue:
                            self._iter_scan_mod(cm_list, self.queue[0])
                    
                    return relative_notes
            

                
    def _get_git_root_initial(self, path_initial_file):
        """! This function will get the git repo of a file

        Args:
            path_initial_file (str): A path to the initial file

        Returns:
            str: The root of the git repo
        """
        git_repo = git.Repo(path_initial_file, search_parent_directories=True)
        git_root = git_repo.git.rev_parse("--show-toplevel")
        
        return git_root
                        
        
    def _get_git_root(self,path_file):
        """! This function will get the the git directory of a certain file

        Args:
            path_file (str): A path to the file

        Returns:
            str: The root of the git directory
        """
        real_path = glob.glob(f"{self.sds.path}/**/{os.path.basename(path_file)}",recursive=True)[0]
        git_repo = git.Repo(real_path, search_parent_directories=True)
        
        git_root = git_repo.git.rev_parse("--show-toplevel")
        
        return git_root



    def _get_superdataset(self):
        """! This function will return the superdataset

        Returns:
            sds: A datalad superdataset
        """
        ds = dl.Dataset(self.dataset)
        sds = ds.get_superdataset()
  
        return sds



    def _get_commit_list(self, commits, run_cmd_commits):
        """! This function will append to run_cmd_commits if there is a DATALAD RUNCMD 
        """ 
        for item in commits:
            if 'DATALAD RUNCMD' in item.message:
                run_cmd_commits.append(item)



    def search_level(self, commits):
        """! This function will search all the notes given an input file

        Args:
            commits (list): A list of commits with datalad run commands
        """
        self._add_queue(self.file)
        self.queue_invar.append(self.file)
        self.queue_level.append(0)
        self._iter_scan_mod(commits, self.file)


    def search(self):
        """! This function will return all the instances of a file search
        repo is the git repo corresponding to a dataset
        """
        all_commits=[]
        dataset_list = []
        
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
            trackline_reverse=self.trackline[::-1]            
            
            self.trackline.clear()
            self.queue.clear()
            self.queue_invar.clear()
            self.queue_level.clear()

        
            self.search_option = 'Forward'
            self.search_level(all_commits)
            trackline_forward=self.trackline
            
            self.trackline = trackline_reverse+trackline_forward


        else:
            self.search_level(all_commits)

        



            

        


def git_log_parse(filename, s_option, l_option):
    """! This function will generate the graphs and objects to represent the filetrack

    Args:
        filename (str): An absolute path to the filename
        s_option (str): A search option (Reverse/Forward)
        g_option (str): A graph display mode (Process/Simple)
    """
    file_track = FileTrack(filename, s_option, l_option) #given a filename and a search option we decide to search for all parents or all childs to fill the file track list
    # profiler.enable()
    file_track.search()
    # profiler.disable()
    # stats = pstats.Stats(profiler).sort_stats('ncalls')
    # stats.print_stats()



    #Once the trackline is calculated we use it to generate a graph in graphviz
    if not file_track.trackline and s_option == 'Reverse':
        st.info('There is no trackline for this specific file, there are no parents to this file')
    elif not file_track.trackline and s_option == 'Forward':
        st.info('There is no trackline for this specific file, there are no childs to this file')
    else:
        st.info('Plotting results')
        # graph = utils.PlotNotes(file_track.trackline, s_option, g_option)
        # graph.plot_notes()

        graph = utils.PlotNotes(file_track.trackline, s_option)
        graph.plot_notes()







if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--filepath", help="Path to file")
    parser.add_argument("-s", "--search_mode", help="Mode to search (Reverse/Forward)", choices=['Reverse','Forward'])
    # parser.add_argument("-d", "--display_mode", help="display type (Process/Simple)", choices=['Process', 'Simple'])
    parser.add_argument("-l", "--level", help="Tree level", type=int)
    parser.add_argument("-a", "--analysis", help="Analysis to apply to nodes", choices=['Centrality','Betweeness'])
    
    args = parser.parse_args()  # pylint: disable = invalid-name

    if args.filepath and args.search_mode and args.display_mode:
        flnm = args.filepath
        search_option = args.search_mode
        plot_option = args.display_mode
        plot_levels = args.level
    else: 
        print("Not all command line arguments were used as input, results might be wrong")
        flnm = st.text_input('Input the file to track')
        search_option = st.selectbox('Search mode', ['Reverse','Forward', 'Bidirectional'])
        analysis_type = st.selectbox('Analysis mode', ['None', 'Degree Centrality', 'Betweeness Centrality'])
        # plot_option = st.selectbox('Display mode', ['Simple','Process'])
        
        plot_levels = st.select_slider(
        'Select a depth level',
        options=[0,1,2,3,4,5,6,7,8,9,10,99999])

        # plot_levels = st.slider('Levels', 0, 10, 1)

    # Sreamlit UI implementation
    

    if flnm:
        git_log_parse(flnm,search_option, plot_levels)