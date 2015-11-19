To build WDB run:
build.sh or build.bat

To run WDB on Unix, do the following:
cd build/classes
$ export INSTANCE_ROOT="."
$ java -cp "../../lib/je.jar:." wdb.WDB
$ java -cp "../../lib/blueprints-core-2.6.0.jar:../../lib/commons-configuration-1.10.jar:../../lib/je.jar:../../lib/titan-core-1.0.0.jar:." wdb.WDB


To run Windows on Unix, do the following:
cd build/classes
$ export INSTANCE_ROOT="."
$ java -cp "..\..\lib\je.jar;." wdb.WDB
