#!/bin/sh

java -classpath bin -Djava.rmi.server.codebase=file:bin/ -Djava.security.policy=server.policy in4391.exercise.a.RMRunner $1 $2