#!/bin/sh

# Arguments
# 1. Location of the main rmi registry to use
# 2. number of jobs
# 3. minimum random duration
# 4. maximum random duration

java -classpath bin -Djava.security.policy=server.policy in4391.exercise.a.RMIJobAdder ${1:-localhost} 10000 10000 30000
