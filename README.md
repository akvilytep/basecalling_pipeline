Project title: pipelines

Description: these pipelines are tailored to handle ONT directories. 
Basecalling_pipeline handles a subset (1-10%) of multidirectory basecalling in one go using pod5s.
Pod5_assist handles subsetting multidirectory pod5 files to generate 1% of filtered pod5 files and name it by flowcell_id. This is then used to do UGE basecalling

Installation instructions: 
pip: python get-pip.py
pandas: pip install pandas 
install .py file in /data/ or usr/bin

Usage: 
cd /data/{yourdirectory}/no_sample
run: python3 /data/pipeline_name

RUN IT FROM data/directory/no_sample
