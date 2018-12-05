# Crane - Stream Processing integrated with 
### 1. A heart-beat membership protocol 
### 2. A slightly-off distributed file system

Run Crane master/slave on each node 
p.s. Master need to be run on the leader node defined in our membership protocol,
which is the one with the lowest id defined in /env.py

~~~
python3 crane_master.py
python3 crane_slave.py
~~~

## Membership protocol commands 
P.S. Can be performed on slave node(s) exclusively

List all members in local list as well as its three neighbors

~~~
ls
~~~

Show local ip address

~~~
self
~~~

Join the group. Need to be called once server is up/want to re-join
~~~
join
~~~

Leave the membership， could re-join later

~~~
decommission
~~~

Leave the group permanently, the same as ctrl+c

~~~
exit
~~~
