#!/bin/bash

#java
echo "JAVA"
javac Pfile.java
java -classpath ".:sqlite-jdbc-3.32.3.2.jar" Pfile

#python
echo ""
echo ""
echo "PYTHON"
python3 Pfile.py

