#!/usr/bin/python

import common
import common2
import random
import argparse
import time

##############
# Globals

# Stores global configuration variables
# NEW VALUES: 
#   -"new"=used to determine if a rebalance request should be honored
#   because the server just started.
#   -"key_lowBound"= points at one's own ID until the view grows larger
#   than the replication number (3). Then, it points to the server hash
#   that is (3) positions counterclockwise from this server. See theory in
#   README.
#   -"rebalancing"= used to reject client RPCs when rebalancing is taking
#   place, as well as to determine appropriate action when a "finalize"
#   RPC is received from the viewleader.
ID=common.hash_key(str(random.random()))
config = {"epoch": None,
          "new": True,
          "port": None,
          "server_hash": ID,
          "last_heartbeat": None,
          "key_lowBound": ID,
          "rebalancing": False}

print ("Server Hash ID: {}".format(config["server_hash"]))

# Stores shared values for get and set commands
store = {}


# Stores 'setr' keys/values, until server receives commit or cancel
pending = {}

# Request or extend a lease from view leaer
def update_lease():
    if config["port"] is None:
        return {}
    config["last_heartbeat"] = time.time()
    res = common.send_receive_range(config["viewleader"], common2.VIEWLEADER_LOW , 
        common2.VIEWLEADER_HIGH , {
            "cmd": "heartbeat",
            "port": config["port"],
            "requestor": config["server_hash"],
        })
    if "error" in res:
        print "Can't update lease: %s" % res["error"]
        return res
    if res.get("status") == 'ok':
        if config["epoch"] is not None and res["epoch"] < config["epoch"]:
            print "Received invalid epoch (%s < %s)" % (res["epoch"], config["epoch"])
            return {"error": "bad epoch"}
        else:
            config["epoch"] = res["epoch"]
    else:
        print "Can't renew lease: %s" % res["status"]
        return res
    return {}

###################
# RPC implementations

# Init function - nop
def init(msg, addr):
    config["port"] = msg["port"]
    update_lease()
    return {}

# set command sets a key in the value store
def set_val(msg, addr):
    key = msg["key"]
    val = msg["val"]
    store[key] = {"val": val}
    print "Setting key %s to %s in local store" % (key, val)
    return {"status": "ok"}

# setr returns a yes or no vote to the client, depending on
# rebalancing status and other pending set requests
def setr_val(msg,addr):
    if config["rebalancing"]:
        return {"vote": "no"}
    key=msg["key"]
    val=msg["val"]
    if key in pending:
        return{"vote":"no"}
    else:
        pending[key]={"val":val}
        print "Awaiting commit"
        return{"vote":"yes", "epoch": config["epoch"]}
        
# commit moves a pending key-value pair into the main store
# if the key in question is in pending. Returns a status message.
def commit(msg,addr):
    key=msg["key"]
    if key in pending:
        store[key]=pending[key]
        del(pending[key])
        print "Commit received"
        return{"status": "ok"}
    else:
        print "Setr failed"
        return{"status": "invalid commit"}
# cancel removes a key from the pending store,
# without transfering it to the main store.
def cancel(msg,addr):
    key=msg["key"]
    if key in pending:
        del(pending[key])
        print "Replicated set canceled"
        return {"status":"setr canceled"}
    else:
        return {"status":"invalid cancel"}

# fetches a key in the value store
def get_val(msg, addr):
    if config["rebalancing"]:
        return {"status": "rebalancing: retry"}
    key = msg["key"]
    if key in store:
        print "Querying stored value of %s" % key
        return {"status": "ok", "value": store[key]["val"],}
    else:
        print "Stored value of key %s not found" % key
        return {"status": "not_found"}


# Returns all keys in the value store
def query_all_keys(msg, addr):
    print "Returning all keys"
    keyvers = [ key for key in store.keys() ]
    return {"result": keyvers}

# Print a message in response to print command
def print_something(msg, addr):
    print "Printing %s" % " ".join(msg["text"])
    return {"status": "ok"}

# accept timed out - nop
def tick(msg, addr):
    #print "Store state:", sorted(store) #TESTING
    return {}

##############
# Rebalance Functions
# Temporary store for data received from other servers
# after a "share" request. Merged into main store and 
# deleted after receiving "finalize" rpc from viewleader.
storeNew={}

# 'rebalance' is an rpc received from the viewleader after 
# every epoch change. Based on the new view, the function determines 
# whether or not the server's store may need to be modified.
# If not, it responds to the viewleader "ok".
# If so, it sends a "share" rpc to all servers in the new
# view, except itself, storing appropriate responses in storeNew.
# When finished, it sends a "done" status to the viewleader.
def rebalance(msg, addr):
    global storeNew
    config["epoch"]=msg["epoch"]
    if len(msg["view"])<2:
        config["new"]=False
        return {"status": "ok"}
    new_lowBound=common.lowBound(config["server_hash"],msg["view"])
    if type(new_lowBound)==tuple:
        (new_lowBound, discard)=new_lowBound
    if config["key_lowBound"]!=new_lowBound or config["new"]:
        config["key_lowBound"]=new_lowBound
        config["new"]=False
        config["rebalancing"]=True
        d=msg["serverdata"]
        serverdata={int(k):v for k,v in d.items()}
        all_sids=msg["view"]
        sids=filter(lambda x : x!=config["server_hash"], msg["view"])
        #don't ask yourself for data
        msg={"cmd": "share", "requestor":config["server_hash"],
        "view":all_sids}
        new_data=common.broadcast_receive(sids,serverdata,msg)
        for item in new_data:
            if "error" in item: 
                print "Share request denied"
            else:
                storeNew.update(item["store"])
        return {"status":"done"}
    else:
        config["new"]=False
        return {"status":"ok"}

# If the server is in a config["rebalancing"] state.
#'finalize' merges storeNew with the main store, then 
# clears storeNew, ends the rebalancing state,
# and sends an "updated" message.
# Otherwise, it sends an "ok" message to the viewleader. 
def finalize(msg, addr):
    global store
    global storeNew
    if config["rebalancing"]:
        print "Old: ",store
        print "New: ",storeNew
        store.clear()
        store.update(storeNew)
        storeNew.clear()
        config["rebalancing"]=False
        return {"status":"updated"}
    else: 
        return {"status":"ok"}

# the opposite of finalize. Cancels the rebalance, hopefully until 
# the view updates and stabilizes and a new rebalance request goes out.
def revert(msg, addr):
    global storeNew
    if config["rebalancing"]:
        storeNew.clear()
        config["rebalancing"]=False
        return{"status":"reverted"}
    else:
        return {"status": "ok"}

# server to server RPC, used to send relevant data to rebalancing
# nodes.
def share(msg, addr):
    sid=msg["requestor"]
    sids=msg["view"]
    is_relevant=common.coverageFn(sid,sids)
    relevants={}
    for k in store.keys():
        if is_relevant(common.hash_key(k)):
            relevants[k]=store[k]
    return {"store":relevants}

##############
# Main program

# RPC dispatcher invokes appropriate function
def handler(msg, addr):
    #print msg #TESTING
    cmds = {
        "init": init,
        "set": set_val,
        "setr": setr_val,
        "get": get_val,
        "getr": get_val,
        "print": print_something,
        "query_all_keys": query_all_keys,
        "timeout": tick,
        "cancel": cancel,
        "commit": commit,
        "rebalance": rebalance,
        "finalize": finalize,
        "revert": revert,
        "share": share
    }
    res =  cmds[msg["cmd"]](msg, addr)

    # Conditionally send heartbeat
    if time.time() - config["last_heartbeat"] >= 10:
        update_lease()

    return res

# Server entry point
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--viewleader', default='localhost')
    args = parser.parse_args()
    config["viewleader"] = args.viewleader
 
    for port in range(common2.SERVER_LOW, common2.SERVER_HIGH):
        print "Trying to listen on %s..." % port
        result = common.listen(port, handler, 10)
        print result
    print "Can't listen on any port, giving up"

if __name__ == "__main__":
    main()