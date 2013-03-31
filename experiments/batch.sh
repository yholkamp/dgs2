#!/bin/sh

##
# Sample batch deployment script that will set up 1 GS nodes on server node1 and 10 RM nodes on server node2.
#
# Note:
#   * Java, ant, git and screen should be installed on the servers
#   * rmiregistry should be running on the machines prior to deployment
#   * /root/wd should contain a git repository containing the DGS2 sources
##

LOG_ID=42
LOG_DIR="/tmp"

servers=(
    "node1 0.0.0.0"
    # RM nodes:
    "node2 0.0.0.0"
)

cmds=(
    "cd ~/wd && git pull && ant && screen -d -m -S test; \
    sleep 0.5; screen -S test -X screen bash -c 'cd ~/wd; ./start-gs.sh <node1-ip> | tee ${LOG_DIR}/${LOG_ID}_gs1-1; bash'"

    # RM nodes:
    "cd ~/wd && git pull && ant && screen -d -m -S test;\
    sleep 1; screen -S test -X screen bash -c 'cd ~/wd; ./start-rm.sh <node2-ip> | tee ${LOG_DIR}/${LOG_ID}_rm1-0; bash';\
    sleep 1; screen -S test -X screen bash -c 'cd ~/wd; ./start-rm.sh <node2-ip> | tee ${LOG_DIR}/${LOG_ID}_rm1-1; bash';\
    sleep 1; screen -S test -X screen bash -c 'cd ~/wd; ./start-rm.sh <node2-ip> | tee ${LOG_DIR}/${LOG_ID}_rm1-2; bash';\
    sleep 1; screen -S test -X screen bash -c 'cd ~/wd; ./start-rm.sh <node2-ip> | tee ${LOG_DIR}/${LOG_ID}_rm1-3; bash';\
    sleep 1; screen -S test -X screen bash -c 'cd ~/wd; ./start-rm.sh <node2-ip> | tee ${LOG_DIR}/${LOG_ID}_rm1-4; bash';\
    sleep 1; screen -S test -X screen bash -c 'cd ~/wd; ./start-rm.sh <node2-ip> | tee ${LOG_DIR}/${LOG_ID}_rm1-5; bash';\
    sleep 1; screen -S test -X screen bash -c 'cd ~/wd; ./start-rm.sh <node2-ip> | tee ${LOG_DIR}/${LOG_ID}_rm1-6; bash';\
    sleep 1; screen -S test -X screen bash -c 'cd ~/wd; ./start-rm.sh <node2-ip> | tee ${LOG_DIR}/${LOG_ID}_rm1-7; bash';\
    sleep 1; screen -S test -X screen bash -c 'cd ~/wd; ./start-rm.sh <node2-ip> | tee ${LOG_DIR}/${LOG_ID}_rm1-8; bash';\
    sleep 1; screen -S test -X screen bash -c 'cd ~/wd; ./start-rm.sh <node2-ip> | tee ${LOG_DIR}/${LOG_ID}_rm1-9; bash'"
)

echo "Starting..."

for idx in ${!servers[*]}
do
    # Bash array hackery
    s=${servers[$idx]}
    set $s
    ip=$2

    # Tell any pre-existing processes to halt
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no root@$ip "killall screen; killall java -HUP" &
done

for idx in ${!servers[*]}
do
    # Bash array hackery
    s=${servers[$idx]}
    set $s
    name=$1
    ip=$2
    cmd=${cmds[$idx]}

    sleep 2
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no root@$ip $cmd &
done

echo "Waiting for processes to finish"
wait

echo "Done"