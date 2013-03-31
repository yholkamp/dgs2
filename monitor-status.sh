#!/bin/sh

java -classpath bin -Djava.security.policy=server.policy in4391.exercise.a.StatusChecker $1 $2
