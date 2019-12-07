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
├── requirements.txt (dependencies to install)
```

#### Steps to run
1. Make sure dependencies are installed as described in the next section
2. Navigate inside ```mini-db``` folder
3. To run it to take commands one-by-one (like any database), run:\
```python3 -m minidb```\
This will show ```minidb>> ``` after which commands can be entered.\
OR\
```python3 -m minidb < input_file.txt```\
This will run all the commands in your input file, for example ```input_file.txt```. 
Note that your input file must be present inside ```mini-db``` folder.

#### Dependencies
1. Python3
2. ```BTrees``` package\
```pip install BTrees```\
More info about this package can be found at: https://pypi.org/project/BTrees. 

3. Numpy\
```pip install numpy```

Alternatively, navigate inside ```mini-db``` folder and run:\
```pip install -r requirements.txt```