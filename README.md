# mp3-ys26-weilinz2

Run server/client on each node

~~~
python3 dfsServer.py
python3 dfsClient.py
~~~

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

Show all files stored on current VM:

~~~
store
~~~

DFS commands on client side:
~~~
put localfilename sdfsfilename:
get sdfsfilename localfilename:
delete sdfsfilename:
ls sdfsfilename
get-versions sdfsfilename numversions localfilename
~~~