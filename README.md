# booj_test

Hi! Thanks for taking the time to read the README! 

## Approach
This section explains some of the design and implementation decisions made during the assignment. My solution uses lxml for XML parsing. Given the relatively small size of the data set, Pandas for data manipulation and loading. Pandas was also chosen for performance. There are several places where high performance Pandas functions were used instead of costly iteration. There are 3 main functions in the solution: `extract_xml:_xml()`, `transform()`, and `load_csv()`. `extract_xml()` is flexible enough to extract from any XML document using it's column configuration (see code for full explanation). `transform()` handles the manipulation of data needed for the zillow data set. It is very specific to the data set in the assignemt, but that's expected since transformations will for the most part be specific to a dataset. `load_csv()` outputs a CSV file. It is flexible taking as parameters a DataFrame, filename and list of columns.

## Extensibilty/Reusability
Object-oriented design principles were not used during the assignment due mainly to time constraints and the fact that without further info on the other types of data sources and formats it is difficult to identify and factor out common functionality. With more time and information on other data sources and formats, an object-oriented design would be a better choice. For example an ETL class could be defined, then the Builder design pattern could be used for the extract, transform and load steps. Interfaces for extract, transform, and load builder classes could be defined, then collections of ETL class instances each with their datatype-specific extract, transform, and load classes could be created. A single load object could be reused across multiple data types as long as a common format for post-transformed data was established. That said, the approach I took is fairly flexible in that it supports extraction from any well formed XML file consisting of a collection of listings, and it is flexible in the load stage as arbitrary data and column headers are supported for writing.
 
## Number of bathrooms
The data set does not contain any information in the BasicDetails/Bathrooms field, however many BasicDetails/FullBathrooms, BasicDetails/HalfBathrooms, BasicDetails/ThreeQuarterBathrooms fields DO contain information. Therefor the Bathrooms field in the output CSV simple counts the number of FullBathrooms, HalfBathrooms, ThreeQuarterBathrooms if Bathrooms is not provided. Note that fractional bathrooms are counted as 1 bathroom (i.e. 2 half baths equals 2 bathrooms not one). This is to avoid ambiguity (i.e. is 1 bathroom a single full bathroom or 2 half baths?). 
Note if no there is no information at all on any bathrooms, a null (nan, None) value is used instead of zero. This is to disambiguate it from "0 bathrooms" (which is unlikely to ever happen, but just in case it does)

## Unit Tests
Unit tests were written to exercise the extract, transform and load functions and their helper functions. Tests were built using Python's unittest framework. There are 3 separate files, one each for extract, transform, and load functions. Tests were written to cover the main functions of the assignment but coverage is not 100% complete. With more time tests would be written covering handling of error cases such as mixing types in a given field, load failures, File Not Found errors and the like. 

## Execution
`etl.py` was successfully run on an AWS t2.micro instance using the Ubuntu image and Python 2.7.12.

    $ python --version
    Python 2.7.12
    ubuntu@ip-10-0-0-6:~/booj_test$ python etl.py
    ubuntu@ip-10-0-0-6:~/booj_test$ ls -lt zillow.csv 
    -rw-rw-r-- 1 ubuntu ubuntu 68992 Feb 18 00:29 zillow.csv
    ubuntu@ip-10-0-0-6:~/booj_test$ cd tests 
    ubuntu@ip-10-0-0-6:~/booj_test$ python extract_unittests.py 
    .......
    ----------------------------------------------------------------------
    Ran 7 tests in 0.006s
    
    OK
    ubuntu@ip-10-0-0-6:~/booj_test$ python transform_unittests.py 
    ....
    ----------------------------------------------------------------------
    Ran 4 tests in 0.024s
    
    OK
    ubuntu@ip-10-0-0-6:~/booj_test$ python load_unittests.py 
    .test.csv
    ..
    ----------------------------------------------------------------------
    Ran 3 tests in 0.023s
    
    OK