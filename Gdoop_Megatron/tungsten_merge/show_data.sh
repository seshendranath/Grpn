#!/bin/sh
hadoop fs -lsr /user/tungsten/staging/$1/$2
hadoop fs -text /user/tungsten/staging/$1/$2/*/*/*
