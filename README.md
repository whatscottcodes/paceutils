# PaceUtils

Python package for running queries on the related database. Contains functions for common indicators calculated at PACE.

## Requirements

All required packages are in the requirements.txt file. There is also an included environment.yml file for setting up a conda environment.

### PaceUtils

Requires a SQLite database set up to the specifications in https://github.com/whatscottcodes/database_mgmt

## Use

See PaceUtils_Guide in the docs folder. Generally a class can be imported using from paceutils import Class and then functions can be run using Class().func(params).
The default database path in the helpers.py file will need to be updated to wherever the SQLite database is located.

