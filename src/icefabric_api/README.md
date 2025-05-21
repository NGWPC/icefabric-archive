CSV files for catchment specific values are too big to be checked in.  They are stored here:  s3://ngwpc-dev/DanielCumpton/divide_csv.tgz  
Download and untar in the icefabric/src/icefabric_api/data directory.  
For now, you can run modules by listing them in the module list defined on line 108 in run_modules.py.  Text files showing all parameters as key/value  
pairs will be written to a files per catchment.  A JSON is output to the terminal.  SFT, SMP, and LASAM csv files still need work to define the parameters properly.  
See icefabric/src/icefabric_api/data/modules.csv for module name strings.
