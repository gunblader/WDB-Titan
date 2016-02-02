To run:
./run.sh

To build and run:
./makerun.sh

Both of these delete any existing database. Consult run.sh to see how to run.sh WDB without deleting any existing database

Building Titan DB
- Relational Model is flexible but does not capture underlying meaning of stored data
- WDB’s class inheritance model captures this meaning
- The WDB SIM implementation uses a Sleepycat database engine to store data in a relational data store
- Reimplementing WDB SIM in Java with Titan database engine to store data in a graph-based data store
- Our implementation allows users to interact with data using the same SIM statements

SIM statements from Homework 7 for test
- big.sim
- small.sim

Example retrieve statements: 
- from SIM_person retrieve *;
- from SIM_emp retrieve *;
- from SIM_project_emp retrieve *, NAME of emps_department, NAME of emps_projects;
- from SIM_manager retrieve *, NAME of mgrs_department;
- from SIM_project retrieve *, NAME of projs_emps, NAME of projs_department;
- from SIM_dept retrieve *, NAME of employees, NAME of projects, NAME of manager;
