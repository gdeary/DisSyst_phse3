# Distributed Hash Table in Python

***

A simple experiment in distributed storage, two-phase commit, and hash allocator in Python. This was the third iteration on a class project, written for COMP 360 "Distributed Systems," Fall 2016, Prof. Jeff Epstein, Wesleyan University.

### State: 
Tested locally on Debian, not currently working on macOS. The touchiest part is the default use of 'localhost'. Definitely could be cleaned up—some unneccessary redundant code and messiness.

### (High Level) Instructions: 
Each process runs in its own terminal window or tab, with appropriate arguments.

 - **Run the viewleader:** it will print its port, any rebalancing commands made and the reason, and the list of rebalancing responses from the servers.
 - **Run a server:** the viewleader should send a rebalance command, which the server will ignore, replying "ok." Run a second server—the viewleader will send a rebalance command again, which the first server will ignore, but the second one will perform, populating its store with everything in the first server, printing its old and new store states, and sending "updated" to the viewleader.
 - **Run a client:** with setr k v (that's "replicated" set argument, with a key/value pair). Try this with both fewer and more than 3 servers active—in the latter case, see which server(s) don't receive the key/value. Kill one of the servers that did receive it, and wait 20 seconds for the rebalance. See which servers have it now.

### Inefficiencies:
- There is room to optimize the management of "share" requests—a server could ask just the 2 servers ahead and 2 behind it, rather than all servers.
- The only place (I believe) where a replication number of 3 is hard-coded into the implementation is the bucket-allocator. Could be made flexible.

### Bugs:
- Sometimes a server gets denied heartbeat after a quick succession of rebalancing. Never when servers are closed one at a time.

## Implementation Notes

### Bucket Allocator:
I chose to use this function in the client program only. It takes a key and a list of server hash ids. If the number of servers is less than replication (3), it returns the same list of servers it received. Otherwise, it takes the hash of the key, and returns the first 3 server ids larger than the key hash, or if there are not 3 larger, then the 2 or 1 or 0 larger, along with the 1 or 2 or 3 smallest server IDs (respectively). This effectively simulates a loop of allocation responsibility.

### Rebalancing
My rebalancing is initiated by a viewleader RPC at the time of each epoch change. When a server receives the RPC, it independently determines whether its store may need to be updated. If it doesn't, it replies to the viewleader with {status: ok}. Such a server can continue to process RPCs.
To determine if its store may need to be rebalanced, the server checks if 
its "key\_lowBound" value has changed in the new view. If it has not, the server need not change anything. This works because, in this algorithm, a server x is only ever responsible for the keys that fall in the hash value circle between the hash of x and hash of the server 3 servers "counterclockwise" from x. The "key_lowBound" value stores this lower range (which indeed may actually be a higher hash number than the given server), and the common.lowBound function calculates the new one. 
Another case in which rebalancing may be deemed unneccessary is when the new view contains fewer than 2 servers. 
When rebalancing is deemed to be necessary for a given server, it's up to that server to request data from each of the other servers.
When a server receives a request to share its data with another server, it first determines which key/value pairs are relevant, using the hash of each key and a "coverageFn" defined function. Given a list of all server IDs and the requestor's ID, this function returns an appropriate filter function, which can be applied to a server's store in determining what keys to send to the requestor. "coverageFn" also makes use of the "lowBound" algorithm for server responsibility.

### Distributed Commit
The distributed commit for setr makes use of 3 server RPCs and 1 viewleader RPC. First the client sends a query_servers command to the viewleader. Then it broadcasts the "setr" message to all servers in the view, storing their responses. 
The client parses the responses, checking for:

- error messages (failed connections)
- no votes
- inconsistent epochs among yes votes

If any of these are present, the server broadcasts a "cancel" RPC to the same group of servers. Otherwise, it broadcasts "commit." 
On the server side, the "pending" dict is used to store setr values in a way similar to a traditional set, and such values are added to the main store only after a "commit" command. 