#!/usr/bin/python

import time
import common
import common2
import threading

##############
# Globals

# Stores global configuration variables
config = {
    "epoch": 0,

    # List of expired leases
    "expired": [],
}

# Stores all server leases
leases = []

# Store all locks
locks = []

###################
# RPC implementations

# Try to acquire a lock
def lock_get(msg, addr):
    lockid = msg["lockid"]
    requestor = msg["requestor"]
    for lock in locks:
        if lock["lockid"] == lockid:
            if len(lock["queue"]) == 0:
                lock["queue"].append(requestor)
                return {"status": "granted"}
            elif lock["queue"][0] == requestor:
                return {"status": "granted"}
            else:
                if requestor not in lock["queue"]:
                    lock["queue"].append(requestor)
                return {"status": "retry"}
    else:
        # this lock doesn't exist yet
        locks.append({"lockid":lockid, "queue": [requestor]})
        return {"status": "granted"}

# Release a held lock, or remove oneself from waiting queue
def lock_release(msg, addr):
    lockid = msg["lockid"]
    requestor = msg["requestor"]
    for lock in locks:
        if lock["lockid"] == lockid:
            if requestor in lock["queue"]:
                lock["queue"].remove(requestor)
                return {"status": "ok"}
    else:
        return {"status": "unknown"}

# Manage requests for a server lease
def server_lease(msg, addr):
    lockid = "%s:%s" % (addr, msg["port"])
    requestor = msg["requestor"]

    remove_expired_leases()

    if msg["requestor"] in config["expired"]:
        return {"status": "deny"}

    for lease in leases:
        if lease["lockid"] == lockid:
            # lease is present

            if time.time() - lease["timestamp"] > common2.LOCK_LEASE:
                # lease expired
                if lease["requestor"] == requestor:
                    # server lost lease, then recovered, but we deny it
                    return {"status": "deny"}
                else:
                    # another server at same address is okay
                    lease["timestamp"] = time.time()
                    lease["requestor"] = requestor
                    config["epoch"] += 1
                    print "Rebalance: transfer address lease"
                    rebalThread = threading.Thread(target=rebalance)
                    rebalThread.start()
                    return {"status": "ok", "epoch": config["epoch"]}
            else:
                # lease still active
                if lease["requestor"] == requestor:
                    # refreshing ownership
                    lease["timestamp"] = time.time()
                    return {"status": "ok", "epoch": config["epoch"]}
                else:
                    # locked by someone else
                    return {"status": "retry", "epoch": config["epoch"]}
    else:
        # lock not present yet
        leases.append({"lockid": lockid, "requestor": requestor, "timestamp": time.time()})
        config["epoch"] += 1
        print "Rebalance: new server"
        rebalThread = threading.Thread(target=rebalance)
        rebalThread.start()
        return {"status": "ok", "epoch": config["epoch"]}

# Check which leases have already expired
def remove_expired_leases():
    global leases
    expired = False
    new_leases = []
    for lease in leases:
        if time.time() - lease['timestamp'] <= common2.LOCK_LEASE:
            new_leases.append(lease)
        else:
            config["expired"].append(lease["requestor"])
            expired = True
    leases=new_leases
    if expired:
        config["epoch"] += 1
        if len(leases)>1:
            print "Rebalance: view reduced"
            rebalThread = threading.Thread(target=rebalance)
            rebalThread.start() 



# THREAD
# rebalance manages a sort of distributed commit on store updates,
# so that no items are lost due to the order of a server's updating 
# and sharing of keys. It deploys finalize or revert, depending one
# the status return messages of each server to the rebalance broadcast.
def rebalance():
    server_ids=[]
    serverdata={}
    for server in leases:
        (h,p)=common.formatHP(server["lockid"])
        val={"host":h, "port":p}
        n=int(server["requestor"])
        server_ids.append(n)
        serverdata[n]=val
    msg={"cmd":"rebalance", "epoch": config["epoch"],
        "group_size":len(leases), "view": server_ids, 
        "serverdata": serverdata}
    responses=common.broadcast_receive(server_ids,serverdata,msg)
    #print responses #Testing
    for r in responses:
        if "error" in r:
            revert(server_ids, serverdata)
            break
    else: 
        finalize(server_ids, serverdata)
# broadcast server RPC, has the effect of finishing the rebalance process
def finalize(sids, data):
    msg={"cmd": "finalize", "epoch": config["epoch"]}
    responses=common.broadcast_receive(sids,data,msg)
    print responses
# broadcast to cancel a rebalance process    
def revert(sids, data):
    msg={"cmd": "revert", "epoch": config["epoch"]}
    responses=common.broadcast_receive(sids,data,msg)
    print"Revert result:", responses #??? TESTING



# Output the set of currently active servers
def query_servers(msg, addr):
    servers = []
    remove_expired_leases()
    for lease in leases:
        ip = lease["lockid"]
        name = lease["requestor"]
        servers.append({"name" : name, "location" : ip})

    return {"result": servers, "epoch": config["epoch"]}

def init(msg, addr):
    return {}

##############
# Main program

# RPC dispatcher invokes appropriate function
def handler(msg, addr):
    cmds = {
        "init": init,
        "heartbeat": server_lease,
        "query_servers": query_servers,
        "lock_get": lock_get,
        "lock_release": lock_release,
    }

    return cmds[msg["cmd"]](msg, addr)

# Server entry point
def main():
 
    for port in range(common2.VIEWLEADER_LOW, common2.VIEWLEADER_HIGH):
        print "Trying to listen on %s..." % port
        result = common.listen(port, handler)
        print result
    print "Can't listen on any port, giving up"

if __name__ == "__main__":
    main()