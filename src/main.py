import streamlit as st
import pandas as pd
import numpy as np
import datalad.api as dl
import datalad_metalad as mdl



st.write("""
#this is a text
""")

dataset = "/Users/pemartin/Scripts/datalad-test/Datalad-101"

status = dl.status(dataset=dataset, untracked='all')
print(status)
st.write(status)


# metadata = dl.meta_dump(dataset)
# print(metadata)
# st.write(metadata)

metadata_runprov = mdl.extractors.metadatalad_runprov(dataset)
print(metadata_runprov)
st.write(metadata_runprov)
