# minidb
A miniature relational database with order that performs basic operations of relational 
algebra such as selection, projection, join, group by, and aggregates (count, sum and avg).

#### Folder Structure
```
mini-db (container folder)
├── data
│   ├── queries
│   ├── sales1 (data)
│   ├── sales2 (data)
├── docs
│   ├── usage.md (sample commands)
├── minidb
│   ├── __init.py 
│   ├── __main__.py (takes command and displays output)
│   ├── argparser.py (class for handling parsing of input text)
│   ├── database.py (class, maintains list of tables & operations bw tables)
│   ├── index.py (class, create and return Hash/Btree index)
│   ├── join.py (class, logic for joins (eq vs non-eq))
│   ├── table.py (class, operations on tables)
│   ├── utils.py (static utility functions)
├── tests
│   ├── mindb_test.py (pytest, tests to check implementation)
├── README.md
├── input_file (commands to run)
├── requirements.txt (dependencies to install)
```
#### Steps to run with reprozip  
```reprounzip directory setup DBProject.rpz project```  
```reprounzip directory run project```  
Note that our ```reprounzip run``` worked on multiple Linux environments and works on https://server.reprozip.org/.  
On certain environments we have experienced issues with ```numpy```. If this occurs, please defer to the steps below to run the source code.
#### Steps to run without reprozip
1. Make sure dependencies are installed as described in the next section
2. Navigate inside ```mini-db``` folder
3. To run it, type the following:
```python3 -m minidb inputFile.txt```. 
(any input file name will work)
This will run all the commands in your input file, for example ```inputFile.txt```. 
Note that your input file must be present inside ```mini-db``` folder.  
The program expects all datasets (ie, sales1, sales2) to reside in the "/data" subdirectory of minid-db.
4. Output for latest run resides in output.txt file.
#### Dependencies
1. Python3
2. BTree package  
 ```pip3 install Btrees``` . 
More info about this package can be found at: https://pypi.org/project/BTrees. \
If there is an issue with permission on CIMS, try with ```pip install --user BTrees```
On linux environment, if you get an error, first run ```sudo apt-get isntall python3-dev```, then run ```pip3 install Btrees```.

3. Numpy\
```pip3 install numpy```

Alternatively, navigate inside ```mini-db``` folder and run:\
```pip install -r requirements.txt```
