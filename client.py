#!/usr/bin/python


import argparse
import common
import common2
import random
import time
import json

def send_cancel(k,v,aloc, serverdata):
    responses=[]
    i=0
    for sid in aloc:
        host=serverdata[sid]["host"]
        port=serverdata[sid]["port"]
        responses.insert( i,
        common.send_receive(host,port, 
            {"key":k,"value": v, "cmd":"cancel"}))

def send_commit(k,v,aloc,serverdata):
    responses=[]
    i=0
    for sid in aloc:
        host=serverdata[sid]["host"]
        port=serverdata[sid]["port"]
        responses.insert( i,
        common.send_receive(host,port, 
            {"key":k,"value": v, "cmd":"commit"}))
    print "Result:",responses


# Client entry point
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', default='localhost')
    parser.add_argument('--viewleader', default='localhost')

    subparsers = parser.add_subparsers(dest='cmd')

    parser_set = subparsers.add_parser('set')
    parser_set.add_argument('key', type=str)
    parser_set.add_argument('val', type=str)

    parser_get = subparsers.add_parser('get')
    parser_get.add_argument('key', type=str)

    parser_print = subparsers.add_parser('print')
    parser_print.add_argument('text', nargs="*")

    parser_query = subparsers.add_parser('query_all_keys')
    parser_server_query = subparsers.add_parser('query_servers')

    parser_lock_get = subparsers.add_parser('lock_get')
    parser_lock_get.add_argument('lockid', type=str)    
    parser_lock_get.add_argument('requestor', type=str)    

    parser_lock_get = subparsers.add_parser('lock_release')
    parser_lock_get.add_argument('lockid', type=str)    
    parser_lock_get.add_argument('requestor', type=str) 

    parser_setr = subparsers.add_parser('setr')
    parser_setr.add_argument('key', type=str)
    parser_setr.add_argument('val', type=str)

    parser_getr = subparsers.add_parser('getr')
    parser_getr.add_argument('key', type=str)

    args = parser.parse_args()

    if args.cmd in ['query_servers', 'lock_get', 'lock_release']:
        while True:
            response = common.send_receive_range(args.viewleader, common2.VIEWLEADER_LOW, common2.VIEWLEADER_HIGH, vars(args))
            if response.get("status") == "retry":
                print "Waiting on lock %s..." % args.lockid
                time.sleep(5)
                continue
            else:
                break
        print response
    elif args.cmd in ['setr', 'getr']:
        query={'viewleader' : args.viewleader, 'cmd' : 'query_servers', 'server' : args.server}
        view=common.send_receive_range(args.viewleader, common2.VIEWLEADER_LOW, common2.VIEWLEADER_HIGH, query)
        if "error" in view:
            print "Viewleader failure:", view
            return()
        else:
            servers=view['result']
        if servers==[]:
            print "No servers available"
        else:
            server_ids=[]
            serverdata={}
            for server in servers:
                (h,p)=common.formatHP(server["location"])
                server["host"]=h
                server["port"]=p
                del server["location"]
                n=int(server["name"])
                server_ids.append(n)
                serverdata[n]=server
            h=common.hash_key(args.key)
            aloc=common.bucket_allocator(args.key, server_ids)
            #list of destination server ids
            if args.cmd=="getr":
                responses=[]
                i=0
                for sid in aloc:
                    responses.insert( i,
                    common.send_receive(serverdata[sid]["host"],serverdata[sid]["port"], vars(args)))
                    if "status" in responses[i] and responses[i]["status"]=="ok":
                        print responses[i]
                        break
                    i+=1
                else: print "No such key found in our system"
            elif args.cmd=="setr":
                responses=common.broadcast_receive(aloc,serverdata,vars(args))
                for r in responses:
                    if "epoch" in r:
                        epoch=r["epoch"]
                for r in responses:
                    if "error" in r:
                        print "Set failed: server connection error"
                        send_cancel(args.key,args.val,aloc,serverdata)
                        break
                    elif r["vote"]=="no":
                        print "Set failed: server voted no"
                        send_cancel(args.key,args.val,aloc,serverdata)
                        break
                    elif r["epoch"]!=epoch:
                        print "Set failed: epoch inconsistency"
                        send_cancel(args.key,args.val,aloc,serverdata)
                        break
                else:
                    send_commit(args.key,args.val,aloc,serverdata)
                    return


    else:
        response = common.send_receive_range(args.server, common2.SERVER_LOW, common2.SERVER_HIGH, vars(args))
        print response

if __name__ == "__main__":
    main()    
