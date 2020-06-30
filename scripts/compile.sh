#!/bin/bash
cd ..
mvn clean package
cp target/project2-1.0.jar scripts
echo "Done"