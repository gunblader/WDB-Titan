#!/bin/bash
cd build/classes
rm -rf thedb 
export INSTANCE_ROOT="."
java -Dlog4j.configuration="log4j.properties" -cp "../../lib/*:." wdb.WDB
#java -Dlog4j.debug -Dlog4j.configuration="log4j.properties" -cp "../../lib/*:." wdb.WDB


