# task_base
Base docker/python setup for running tasks, for use in calculating the Human Impact Index and Species Conservation Landscapes.

This repository contains the framework for use by all HII and SCL repos:
1. base Dockerfile to be inherited
2. base Python classes to be subclassed 

It is not meant to be run or instantiated directly. An example for how to use this repo can be found at  
https://github.com/SpeciesConservationLandscapes/task_hii_popdens  
Based on this example, to create a new task:
1. create a new repo using the convention of starting names with `task_hii_` or `task_scl_`
2. create a new Dockerfile in its root that can be more or less a copy of  
https://github.com/SpeciesConservationLandscapes/task_hii_popdens/blob/master/Dockerfile  
   and add additional libraries as necessary
3. within a src/ dir, create a `task.py` and any other Python files needed
4. In the repo root, create or copy a `.env` file that contains at minimum a `SERVICE_ACCOUNT_KEY` entry

## Classes
- `Task`: base class defining `taskdate` and other key properties - use for basic pipeline tasks not involving Earth
 Engine
- `EETask`: base Earth Engine task - sufficient for all non-species-specific EE tasks
- `HIITask`: use for Human Impact Index-specific EE tasks
- `SCLTask`: use for species-specific EE tasks

## Running locally
To run locally, copy into your root either:  
a) [recommended] a .env file  containing stringified GCP service account authentication details, or   
b) a `.config` dir containing your Earth Engine credentials file

Example commands below use `task_hii_popdens` as an example inheriting from this repo.  
Note that all tasks require a `taskdate` argument, and other arguments may be available or required (not shown here); 
for examples see existing task `README`s.  
Include the `-v $PWD/.git:/app/.git` argument to write the SHA of the git commit 
used to produce EE assets to their properties. 
- To build docker image:  
`docker build --pull --no-cache -t scl3/task_hii_popdens .`
- To run normally, with GCP service account credentials:  
`docker run --rm -it --env-file ./.env -v $PWD/src:/app -v $PWD/.git:/app/.git scl3/task_hii_popdens python task.py`
- To start a bash shell within the container for development, running `python task.py` from there:  
`docker run -it --env-file ./.env -v $PWD/src:/app -v $PWD/.git:/app/.git scl3/task_hii_popdens bash`
- To run with your personal ee credentials stored in a .config dir that you've copied from your user dir:  
`docker run -it -v $PWD/.config:/root/.config -v $PWD/src:/app -v $PWD/.git:/app/.git scl3/task_hii_popdens python task.py`

## Cloud deployment
Each task will, with appropriate credentials, run locally. To be available for other environments, 
particularly cloud deployment, create a docker repository and push to it or configure automated builds. 
The public Docker repos used as part of the HII/SCL project are available at 
https://cloud.docker.com/u/scl3/repository/list  

The Github and Docker repositories referenced herein are used by the GCP pipeline (also open source) available at
https://github.com/SpeciesConservationLandscapes/gcp-pipeline-devops
With the appropriate GCP configuration, tasks based on this repository can be run serially and in parallel; 
see README above for details.

### License
Copyright (C) 2022 Wildlife Conservation Society
The files in this repository  are part of the task framework for calculating 
Human Impact Index and Species Conservation Landscapes (https://github.com/SpeciesConservationLandscapes) 
and are released under the GPL license:
https://www.gnu.org/licenses/#GPL
See [LICENSE](./LICENSE) for details.
