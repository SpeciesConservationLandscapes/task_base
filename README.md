# task_base
Base docker/python setup for running tasks and Earth Engine tasks

This repo contains the framework for use by all HII and SCL repos:
1. base Dockerfile to be inherited
2. base Python classes to be subclassed 

It is not meant to be run or instantiated directly. An example for how to use this repo can be found at  
https://github.com/SpeciesConservationLandscapes/task_hii_popdens  
Based on this example, to create a new task:
1. create a new repo using the convention of starting names with `task_hii_` or `task_scl_`
2. create a new Dockerfile in its root that can be more or less a copy of  
https://github.com/SpeciesConservationLandscapes/task_hii_popdens/blob/master/Dockerfile  
3. within a src/ dir, create your python files

To be available for cloud deployment, create a docker repo at  
https://cloud.docker.com/u/scl3/repository/list  
and configure an automated build using the `:latest` tag of the new git repo.

## Running locally
To run locally, copy into your root either:  
a) [recommended] a .env file  containing stringified GCP service account authentication details, or   
b) a `.config` dir containing your Earth Engine credentials file

Example commands below use `task_hii_popdens` as an example inheriting from this repo.   
Include the `-v $PWD/.git:/app/.git` argument to write the SHA of the git commit 
used to produce EE assets to their properties. 
- To build docker image:  
`docker build --pull --no-cache -t scl3/task_hii_popdens .`
- To run with your personal ee credentials stored in a .config dir that you've copied from your user dir:  
`docker run -it -v $PWD/.config:/root/.config -v $PWD/.git:/app/.git scl3/task_hii_popdens python task/hii_popdens.py`
- To run with GCP service account credentials:  
`docker run -it --env-file ./.env -v $PWD/.git:/app/.git scl3/task_hii_popdens python /app/hii_popdens.py`
- To additionally map host code dir to container app dir for development, running `python /app/hii_popdens.py` within
 container  
`docker run -it --env-file ./.env -v $PWD/src:/app -v $PWD/.git:/app/.git scl3/task_hii_popdens sh`

## Classes
- `Task`: base class defining `taskdate` and other key properties - use for basic pipeline tasks not involving Earth
 Engine
- `EETask`: base Earth Engine task - sufficient for all non-species-specific EE tasks
- `HIITask`: use for Human Footprint-specific EE tasks
- `SCLTask`: use for species-specific EE tasks
