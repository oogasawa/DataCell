#!/bin/bash

mvn javadoc:javadoc
rm -Rf ~/public_html/javadoc/DataCell
mv target/site ~/public_html/javadoc/DataCell
