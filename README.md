# DGS^2 - Distributed Grid Scheduler Simulator

The DGSS project implements a distributed version of a virtual grid system simulator, which includes a system of multiple
schedulers with one elected master node to perform matchmaking. All system state is replicated over all nodes, and the
system is fault-tolerant in the face of random node failures. Experimental results show decent scalability results,
with minimal overhead in scenario with 5 grid scheduler (GS) nodes, 80 resource manager (RM) nodes and a workload of 40,000 jobs.

Due to the distributed nature of the implementation, any number of failures of GS or RM nodes will not result in a system failure,
as long as at least one GSNode survives the failures at all times. The system handles this by electing one GSNode to be the 'master' of the system,
which will take on the responsibility of sequencing any messages sent through the system, providing a global ordering of events and an easy way to broadcast messages.

This project has been developed for the IN4391 - Distributed Computing Systems course at Delft University of Technology. The report submitted for this course [is also available](http://yholkamp.github.com/dgs2/report.pdf). 

## Requirements

* Minimum: Java 1.6, recommend: Java 1.7 (for insight into the system and process CPU usage of nodes)
* Ant
* Optional: screen

## Usage

### System setup

1. Edit the `system.properties` file to contain all the external IP addresses of all of the machines that will participate in the system.
2. Deploy the code to the machines that will participate.
3. Execute `ant`.
4. On every participating machine, execute `rmiregistry`.
5. Start any number of GSNodes, using `./start-gs.sh <external ip>`.
6. Start any number of RMNodes, using `./start-rm.sh <external ip`.

### System usage

* Adding jobs
    * Execute `./add-jobs.sh` in order to add a configurable number of jobs to the system, or
    * Press `return` in the terminal window of the `./start-rm.sh` to immediately add a job to the RMNode.
* Monitoring
    1. Execute `./monitor-status.sh`
    2. Press `return` in order to retrieve the current job queue size, message queue size and CPU usage of all of the registered nodes in the system.
