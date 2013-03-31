#!/bin/sh

java -classpath bin -Djava.rmi.server.codebase=file:bin/ -Djava.security.policy=server.policy -Djava.security.debug=failure in4391.exercise.a.GSRunner $1 $2
