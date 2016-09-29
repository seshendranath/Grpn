#!/bin/sh
hadoop fs -lsr /user/tungsten/hadooprep_test_staging/$1/$2
hadoop fs -text /user/tungsten/hadooprep_test_staging/$1/$2/*/*/*
