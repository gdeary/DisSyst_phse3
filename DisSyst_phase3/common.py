import json
import socket
import struct
import hashlib
import common2

MAX_MESSAGE_SIZE = 8192
HASH_MAX = 100000

# Encode and send a message on an open socket
def send(sock, message):
    message = json.dumps(message).encode()

    nlen = len(message)
    if nlen >= MAX_MESSAGE_SIZE:
        return {"error": "maxmimum message size exceeded"}
    slen = struct.pack("!i", nlen)

    if sock.sendall(slen) is not None:
        return {"error", "incomplete message"}
    if sock.sendall(message) is not None:
        return {"error", "incompletely sent message"}

    return {}

# Expect a message on an open socket
def receive(sock):
    nlen = sock.recv(4, socket.MSG_WAITALL)
    if not nlen:
        return {"error": "can't receive"}

    slen = (struct.unpack("!i", nlen)[0])
    if slen >= MAX_MESSAGE_SIZE:
        return {"error": "maximum response size exceeded"}
    response = sock.recv(slen, socket.MSG_WAITALL)

    return json.loads(response.decode())

def send_receive_range(host, port_low, port_high, message):
    for port in range(port_low, port_high):
        response = send_receive(host, port, message)
        if "error" in response:
            continue
        else:
            return response
    else:
        return {"error": "can't connect to %s" % host}


# Encapsulates the send/receive functionality of an RPC client
# Parameters
#   host, port - host and port to connect to
#   message - arbitrary Python object to be sent as message
# Return value
#   Response received from server
#   In case of error, returns a dict containing an "error" key
def send_receive(host, port, message):
    sock = None
    try:
        sock = socket.create_connection((host, port), 5)
        if not sock:
            return {"error": "can't connect to %s:%s" % (host, port)}

        send_result = send(sock, message)
        if "error" in send_result:
            return send_result

        receive_result = receive(sock)
        return receive_result

    except ValueError as e: 
        return {"error": "json encoding error %s" % e}
    except socket.error as e:
        return {"error": "can't connect to %s:%s because %s" % (host, port, e)}
    finally:
        if sock is not None:
            sock.close()

# A simple RPC server
# Parameters
#   port - port number to listen on for all interfaces
#   handler - function to handle respones, documented below
#   timeout - if not None, after how many seconds to invoke timeout handler
# Return value
#   in case of error, returns a dict with "error" key
#   otherwise, function does not return until timeout handler returns "abort"
#
# the handler function is invoked by the server in response
# handler is passed a dict containing a "cmd" key indicating the event
# the following are possible values of the "cmd" key:
#    init: the port has been bound, please perform server initializiation
#    timeout: timeout occurred
#    anything else: RPC command received
# the return value of the handler function is sent as an RPC response
def listen(port, handler, timeout=None):
    bindsock = None
    try:
        bindsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        bindsock.bind(('', port))
        bindsock.listen(1)
        if timeout:
            bindsock.settimeout(timeout)

        if "abort" in handler({"cmd":"init", "port": port}, None):
            return {"error": "listen: abort in init"}

        sock = None
        addr = None

        while True:
            try:
                sock, (addr, accepted_port) = bindsock.accept()

                nlen = sock.recv(4, socket.MSG_WAITALL)
                if not nlen:
                    print "listen: invalid length"
                    continue

                slen = struct.unpack("!i", nlen)[0]
                if slen >= MAX_MESSAGE_SIZE:
                    print "listen: invalid message size %s" % slen
                    continue
                msg = sock.recv(slen, socket.MSG_WAITALL)
                if not msg:
                    print "listen: receive failed"
                    continue

                jsonmsg = json.loads(msg)

                try:
                    response = handler(jsonmsg, addr)
                    if "abort" in response:
                        print "listen: abort"
                        return response
                        break
                except Exception as e: #Break for testing
                    print "listen: handler error: %s" % e
                    continue

                json_response = json.dumps(response)
                slen = struct.pack("!i", len(json_response))
                sock.sendall(slen)
                sock.sendall(json_response)
            except socket.timeout:
                if "abort" in handler({"cmd":"timeout"}, None):
                    return {"error": "listen: abort in timeout"}
            except ValueError as e: #Break for testing
                print "listen: json encoding error %s" % e
            except socket.error as e:
                print "listen: socket error %s" % e
            finally:
                if sock is not None:
                    sock.close()
    except socket.error as e:
        return {"error": "can't bind %s" % e}
    finally:
        if bindsock is not None:
            bindsock.close()

# hash function for creating server IDs and allocating keys.
def hash_key(d):
    sha1 = hashlib.sha1(d)
    return int(sha1.hexdigest(),16) % HASH_MAX

# function to convert the formatted "address:port" string
# to an (address, port) tuple.
def formatHP(hp):
    host=''
    port=''
    precolon=True
    for c in hp:
        if precolon:
            if c==':':
                precolon=False
            else: host+=c
        else:
            port+=c
    return((host, int(port)))

# the bucket allocator is used by the client to determine which
# servers should store a key. k is the key, sids is the list of 
# all server IDs.
def bucket_allocator(k,sids):
    if len(sids)<common2.REPLICATION:
        return sids
    h=hash_key(k)
    sids.sort()
    if h>sids[-1]:
        return sids[0:3]
    elif h>sids[-2]:
        return [sids[-1], sids[0], sids[1]]
    elif h>sids[-3]:
        return [sids[-2], sids[-1], sids[0]]
    else:
        for i in range(len(sids)):
            if h<sids[i]:
                return[sids[i],sids[i+1], sids[i+2]]

# lowBound takes a server ID sid and the view of all server IDs,
# including sid, and returns the server ID that is REPLICATION number
# of steps to the "left" (or counterclockwise, or whichever metaphor).
def lowBound(sid,sids):
    if len(sids)>common2.REPLICATION: 
        sids.sort()
        i=sids.index(sid)
        return (sids[i-common2.REPLICATION], i)
    else: 
        return sid

# 'i' represents the position of the server in question 
# within the ordered list of server hashes. If i-replication
# is a negaitive number, then the server's coverage area
# loops around the zero line. 
def coverageFn(sid,sids):
    if len(sids)>common2.REPLICATION: 
        (n3ighbor,i)=lowBound(sid,sids)
        if i-common2.REPLICATION < 0:
            return lambda x : True if x>n3ighbor or x<sid else False
        else:
            return lambda x : True if x>n3ighbor and x<sid else False
    else:
        return lambda x : True

# broadcast receive sequentially performs send_receive on a list of servers, and 
# then returns a lost of their responses.
def broadcast_receive(sids, serverdata, msg):
    responses=[]
    i=0
    for sid in sids:
        host=serverdata[sid]["host"]
        port=serverdata[sid]["port"]
        responses.insert(i,
        send_receive(host,port, msg))
        if "error" in responses[i]:
            #print {"fail":responses[i], "server":sid} #TESTING
            pass
        i+=1
    return responses



            


