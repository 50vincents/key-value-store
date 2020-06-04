import os, requests, json, threading
import hashlib
from flask import Flask, jsonify, request, Response
import sys

app = Flask(__name__)

view = os.getenv('VIEW')
socket_address = os.getenv('SOCKET_ADDRESS')
shard_count = os.getenv('SHARD_COUNT')

key_value_store = {}
vector_clock = {}
# Shard view is the state of all shards in system
shard_view = {}
# Shard member_list is the list of members of the shard this replica is in
shard_member_list = []
# This is the id of the shard this replica is in
replica_shard_id = None
# This is how many times the store has been resharded
reshard_count = 0
# This is how many times a new replica has been added
add_count = 0

background_process = threading.Thread()

# Debugging endpoints
@app.route("/debug-vec-clock", methods=["GET"])
def debug_vc():
    return jsonify({"vc":vector_clock})

@app.route("/debug-store", methods=["GET"])
def debug_store():
    return jsonify({"store":key_value_store})

@app.route("/debug-shard-view", methods=["GET"])
def debug_shard():
    return jsonify({"shard-view":shard_view, "shard-id":replica_shard_id, "reshard-count":reshard_count})

# This is for checking if incoming clock is "ahead in time" 
def up_to_date(incoming_clock):
    global vector_clock
    if (incoming_clock is None):
        return True
    else:
        for address in shard_member_list:
            if incoming_clock[address] > vector_clock[address]:
                return False
    return True

# This is for checking if the replica is up to date for maintaining causal consistency
def replica_up_to_date(incoming_clock, sender):
    global vector_clock
    if (incoming_clock[sender] != 1 + incoming_clock[sender]):
        return False
    else:
        for address in shard_member_list:
            if address != sender and incoming_clock[address] > vector_clock[address]:
                return False
    return True
    
def hash_mod_n(key):
    n = len(shard_view.keys())
    hash_obj = hashlib.md5(key.encode())
    hash_digest = hash_obj.hexdigest()
    key_id = (int(hash_digest, 16) % n) + 1
    return key_id

@app.route("/key-value-store/<string:key>", methods = ['PUT'])
def put(key):
    global view
    global vector_clock
    global key_value_store
    global shard_view
    global replica_shard_id
    global reshard_count
    global add_count

    # if the current replica does not share the same shardID as the key,
    # forward the request to a member of the correct shardID
    keyID = hash_mod_n(key)
    if keyID != replica_shard_id:
        member = shard_view[keyID][0] # pick the first member?
        req = requests.put('http://' + member + '/key-value-store/' + key, json=request.json)
        return req.content, req.status_code

    value_request = request.json
    incoming_metadata = value_request.get('causal-metadata')

    #make sure view is up to date
    check_view()

    if incoming_metadata=="" or incoming_metadata['reshard-count'] < reshard_count or incoming_metadata['add-count'] < add_count or up_to_date(incoming_metadata['vector_clock']):
        response = do_put(value_request, key)
        return response
    else:
        get_most_up_to_date()
        response = do_put(value_request, key)
        return response

# This actually does the put operation
def do_put(value_request, key):
    global vector_clock
    global key_value_store
    global reshard_count
    global add_count
    if len(value_request) == 0 or value_request['value'] is None:
        response = {"error":"Value is missing", "message":"Error in PUT"}
        return jsonify(response),400
    if key in key_value_store:
        vector_clock[socket_address]+=1 
        key_value_store[key] = value_request['value']
        metadata_object = {"value":value_request['value'], "vector_clock":vector_clock, "sender":socket_address}
        response_metadata_object = {"vector_clock": vector_clock, "reshard-count":reshard_count, "add-count":add_count}

        broadcast_request("PUT", metadata_object, key)

        response = {"message": "Updated successfully", "causal-metadata":response_metadata_object, "shard-id":hash_mod_n(key)}
        return jsonify(response),200
    else:
        if len(key) > 50: 
            response = {"error":"Key is too long", "message":"Error in PUT"}
            return jsonify(response), 400
        else:
            vector_clock[socket_address]+=1
            key_value_store[key] = value_request['value']
            metadata_object = {"value":value_request['value'], "vector_clock":vector_clock, "sender":socket_address}
            response_metadata_object = {"vector_clock": vector_clock, "reshard-count": reshard_count, "add-count":add_count}

            broadcast_request("PUT", metadata_object, key)
            
            response = {"message":"Added successfully", "causal-metadata":response_metadata_object, "shard-id":hash_mod_n(key)}
            return jsonify(response), 201
        
# Queries every replica in its view for their vector clock and key-value store 
# and updates its state if a fellow replica is more up-to-date
def get_most_up_to_date():
    global view
    global vector_clock
    global key_value_store
    for address in shard_member_list:
        if address != socket_address:
            response = requests.get('http://'+address+'/get_vec_clock', json={"sender":socket_address})
            replica_vec_clock = response.json()['vc']
            if not up_to_date(replica_vec_clock):
                vector_clock = replica_vec_clock
                response = requests.get('http://'+address+'/get_store')
                key_value_store = response.json()['store']

# Broadcast a request to all replicas
def broadcast_request(method, metadata_object, key):
    global shard_member_list
    for address in shard_member_list:
        if address != socket_address:
            if method == "PUT":
                requests.put('http://'+address+"/key-value-broadcast/"+key, json=metadata_object)
            elif method == "DELETE":
                requests.delete('http://'+address+"/key-value-broadcast/"+key, json=metadata_object)


# Prunes any replicas in view that are down
def check_view():
    global view
    view_list = view.split(",")
    view_list_copy = view_list.copy()
    for address in view_list:
        if address != socket_address:
            try:
                requests.get('http://' + address + '/alive-check', timeout=0.5)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                view_list_copy.remove(address)
                # if address in vector_clock:
                #     del vector_clock[address]
    view_list = view_list_copy
    view = ",".join(view_list)

@app.route("/get_vec_clock", methods=['GET'])
def get_vec_clock():
    global view
    request_value = request.json
    if request_value['sender'] not in view.split(","):
        view += ","+request_value['sender']
    return jsonify({"vc": vector_clock})

@app.route("/get_store", methods=['GET'])
def get_store():
    return jsonify({"store": key_value_store})

# Update key-value store to maintain consistency with a write request sent to another replica
@app.route("/key-value-broadcast/<key>", methods=['PUT'])
def put_broadcast(key):
    global view
    request_value = request.json

    if request_value['sender'] not in vector_clock:
        vector_clock[request_value['sender']] = 0
    if request_value['sender'] not in view.split(","):
        view += ","+request_value['sender']

    if replica_up_to_date(request_value['vector_clock'], request_value['sender']):
        key_value_store[key] = request_value['value']
        return "delivered"
    else:
        get_most_up_to_date()
        key_value_store[key] = request_value['value']
        return "delivered"

@app.route("/key-value-broadcast/<key>", methods=['DELETE'])
def delete_broadcast(key):
    global view
    request_value = request.json

    if request_value['sender'] not in vector_clock:
        vector_clock[request_value['sender']] = 0
    if request_value['sender'] not in view.split(","):
        view += ","+request_value['sender']

    if replica_up_to_date(request_value['vector_clock'], request_value['sender']):
        del key_value_store[key]
        return "delivered"
    else:
        get_most_up_to_date()
        del key_value_store[key]
        return "delivered"

@app.route('/key-value-store/<key>', methods=['GET'])
def key_get(key):
    global view
    global vector_clock
    global replica_shard_id
    global key_value_store
    global shard_view
    global reshard_count
    global add_count

    check_view()
    get_most_up_to_date()

    # find shard ID corresponding to key
    shardID = hash_mod_n(key)
    # if it is same as <shard ID>
    if shardID == replica_shard_id:
        # respond to client AKA process request
        if key in key_value_store and key_value_store[key] is not None:
            response_metadata_object = {'vector_clock' : vector_clock, "reshard-count":reshard_count, "add-count":add_count}
            return jsonify({"message":"Retrieved successfully", "causal-metadata":response_metadata_object, "value":key_value_store[key]}), 200
        else:
            return jsonify({"error":"Key does not exist","message":"Error in GET"}), 404
    # forward GET request to one of members of <shard ID>
    else:
        # get members of shard id
        member = shard_view[shardID][0] # pick the first member?
        response = requests.get('http://' + member + '/key-value-store/' + key, json=request.json)
        return response.content, response.status_code


@app.route('/key-value-store/<string:key>', methods=['DELETE'])
def key_delete(key):
    global view
    global vector_clock
    global key_value_store
    global reshard_count
    global add_count

    # if the current replica does not share the same shardID as the key,
    # forward the request to a member of the correct shardID
    keyID = hash_mod_n(key)
    if keyID != replica_shard_id:
        member = shard_view[keyID][0] # pick the first member?
        req = requests.delete('http://' + member + '/key-value-store/' + key, json=request.json)
        return req.content, req.status_code


    value_request = request.json
    incoming_metadata = value_request.get('causal-metadata')

    check_view()

    if incoming_metadata=="" or incoming_metadata['reshard-count'] < reshard_count or incoming_metadata["add-count"] < add_count or up_to_date(incoming_metadata['vector_clock']):
        response = do_delete(value_request, key) 
        return response
    else:
        get_most_up_to_date()
        response = do_delete(value_request, key)
        return response

# Performs delete operation
def do_delete(value_request, key):
    global vector_clock
    global key_value_store
    global reshard_count
    global add_count

    # key exists retrieve and delete
    if key in key_value_store and key_value_store[key] is not None:
        vector_clock[socket_address]+=1
        del key_value_store[key]
        metadata_object = {"value":None, "vector_clock":vector_clock, "sender":socket_address}
        response_metadata_object = {"vector_clock": vector_clock, "reshard-count":reshard_count, "add-count":add_count}

        # broadcast delete request to other replicas
        broadcast_request("DELETE", metadata_object, key)

        response = {"message":"Deleted successfully", "causal-metadata":response_metadata_object, "shard-id":hash_mod_n(key)}
        return jsonify(response), 200
    # key dne
    else:
        response = {"error":"Key does not exist", "message":"Error in DELETE"}
        return jsonify(response), 404

@app.route('/key-value-store-view', methods=['PUT'])
def view_put():
    global view
    view_list = view.split(",")  # view updated to only alive replicas
    value = request.json
    sock_add = value['address']
    if sock_add in view_list:
        return jsonify({"error":"Socket address already exists in the view","message":"Error in ADD REPLICA"}), 404
    else:
        # try:
        #     view_list.append(sock_add)
        # except ValueError:
        #     return jsonify({"error":"Could not update view","message":"Error in ADD REPLICA"}), 404
        view_list.append(sock_add)    
    view = ','.join(view_list)
    return jsonify({"message":"Replica added successfully to the view"}), 200

@app.route('/key-value-store-view', methods=['GET'])
def view_get():
    global view
    view_list = view.split(",")
    view_list_copy = view_list.copy()
    for address in view_list:
        try:
            requests.get('http://' + address + '/alive-check', timeout=0.8)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            view_list_copy.remove(address)
            # Broadcast delete of the address to all sockets in view
            for a in view_list:
                if a != socket_address:
                    # Set the data field
                    data_field = '{"%s": "%s"}' % (a, address)
                    # Request the delete
                    try:
                        requests.delete('http://' + a + '/key-value-store-view', data=data_field)
                    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                        pass
    view_list = view_list_copy     
    view = ','.join(view_list)
    return jsonify({"message":"View retreived successfully", "view":view}), 200
        
@app.route('/key-value-store-view', methods=['DELETE'])
def view_delete():
    global view
    # Parse the data field
    data = json.loads(request.data)
    # Extracts the replica's socket, and the socket to be deleted from that replica's view
    # **only works assuming the data field only contains a single key-value pair**
    [(sock_src, sock_del)] = data.items()

    # If not deleting from this replica, forward the delete request to the correct replica
    if sock_src != socket_address:
        req = requests.delete('http://' + sock_src + '/key-value-store-view', data=request.data)
        return req.content, req.status_code

    # Else delete the sock_del from the view of this replica
    view_list = view.split(",")
    try:
        view_list.remove(sock_del)
    except ValueError:
        # If the socket address doesnt exist in the view
        return jsonify({"error":"Socket address does not exist in the view","message":"Error in DELETE"}), 404

    # Socket address successfully deleted from view
    view = ','.join(view_list)
    return jsonify({"message":"Replica deleted successfully from the view"}), 200
        

@app.route('/alive-check', methods=['GET'])
def alive_check():
    return "alive"

def periodic_put():
    global background_process
    global view
    global socket_address
    for address in view.split(","):
        if address != socket_address:
            try:
                value = {"address": socket_address}
                requests.put("http://"+address+"/key-value-store-view", json=value)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                pass
    background_process = threading.Timer(1, periodic_put, ())
    background_process.start()

def background():
    global background_process
    background_process = threading.Timer(1, periodic_put, ())
    background_process.start()

@app.route('/key-value-store-shard/shard-ids', methods=['GET'])
def get_ids():
    return jsonify({"message":"Shard IDs retrieved successfully","shard-ids":list(shard_view.keys())}), 200

@app.route('/key-value-store-shard/node-shard-id', methods=['GET'])
def get_shard_id():
    return jsonify({"message":"Shard ID of the node retrieved succesfully", "shard-id":replica_shard_id}), 200

@app.route('/key-value-store-shard/shard-id-members/<shard_id>', methods=['GET'])
def get_shard_members(shard_id):
    index = int(shard_id)
    return jsonify({"message":"Members of shard ID retrieved successfully", "shard-id-members":shard_view[index]})

@app.route('/get_store_count', methods=['GET'])
def get_store_count():
    get_most_up_to_date()
    return jsonify({"store-count":str(len(key_value_store.keys()))})

@app.route('/key-value-store-shard/shard-id-key-count/<shard_id>', methods=['GET'])
def get_key_count(shard_id):
    shard_id = int(shard_id)
    if shard_id == replica_shard_id:
        return jsonify({"message":"Key count of shard ID retrieved succesfully","shard-id-key-count":len(key_value_store.keys())}), 200
    else:
        retrieved_store_count = ""
        for address in shard_view[shard_id]:
            try:
                response = requests.get('http://'+address+'/get_store_count')
                retrieved_store_count = response.json()['store-count']
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                pass
        return jsonify({"message":"Key count of shard ID retrieved succesfully","shard-id-key-count":int(retrieved_store_count)}), 200

@app.route('/put-shard-member-broadcast/<shard_id>', methods=['PUT'])
def put_shard_broadcast(shard_id):
    global shard_view
    global shard_member_list
    global vector_clock
    global add_count
    add_count += 1
    value_request = request.json
    add_address = value_request['socket-address']
    shard_id = int(shard_id)
    if add_address not in shard_view[shard_id]:
        shard_view[shard_id].append(add_address)
    if add_address not in vector_clock:
        vector_clock[add_address] = 0
    if add_address in shard_view[replica_shard_id]:
        shard_member_list.append(add_address)
    return "added"

@app.route('/set-store-and-id', methods=['PUT'])
def set_store_and_id():
    global replica_shard_id
    global shard_view
    global vector_clock
    global shard_member_list
    global key_value_store
    global reshard_count
    global add_count

    value_request = request.json
    replica_shard_id = int(value_request["new-shard-id"])
    shard_view = value_request["new-shard-view"]
    shard_view = {int(k):[str(i) for i in v] for k,v in shard_view.items()}
    #print(shard_view, file=sys.stderr)
    vector_clock = value_request["new-vec-clock"]
    vector_clock = {str(k):int(v) for k,v in vector_clock.items()}
    vector_clock[socket_address] = 0
    key_value_store = value_request["new-store"]
    shard_member_list = shard_view[replica_shard_id]
    reshard_count = int(value_request["new-reshard-count"])
    add_count = int(value_request["new-add-count"])
    return "done"

@app.route('/key-value-store-shard/add-member/<shard_id>', methods=['PUT'])
def add_member(shard_id):
    global shard_member_list
    global view
    global shard_view
    global reshard_count
    global vector_clock
    global add_count

    value_request = request.json
    add_address = value_request['socket-address']
    shard_id = int(shard_id)
    shard_view[shard_id].append(add_address)
    
    if shard_id == replica_shard_id:
        shard_member_list.append(add_address)

    new_replica_store = {}
    new_replica_vec_clock = {}
    for address in shard_view[shard_id]:
        try:
            response = requests.get('http://'+address+'/get_store')
            new_replica_store = response.json()['store']
            response = requests.get('http://'+address+'/get_vec_clock', json={"sender":socket_address})
            new_replica_vec_clock = response.json()['vc']
            break
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            pass

    add_count += 1
    if add_address not in shard_view[shard_id]:
        shard_view[shard_id].append(add_address)
    new_replica_state = {"new-store":new_replica_store, "new-vec-clock": new_replica_vec_clock, "new-shard-id":shard_id, "new-shard-view":shard_view, "new-reshard-count":reshard_count, "new-add-count":add_count}
    requests.put("http://"+add_address+"/set-store-and-id", json=new_replica_state)

    vector_clock[add_address] = 0

    for address in view.split(","):
        if address != socket_address:
            try:
                value = {"socket-address":add_address}
                requests.put("http://"+address+"/put-shard-member-broadcast/"+str(shard_id), json=value)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                pass
    return jsonify({}), 200

@app.route('/key-value-store-shard/reshard', methods=['PUT'])
def reshard():
    global view
    global shard_view
    global shard_member_list
    global replica_shard_id
    global vector_clock
    global reshard_count
    global key_value_store

    value_request = request.json
    shard_count = int(value_request['shard-count'])
    check_view()
    view_list = view.split(",")
    if 2*shard_count > len(view_list):
        return jsonify({"message":"Not enough nodes to provide fault-tolerance with the given shard count!"}), 400

    # Reset vector clock
    for address in view_list:
        vector_clock[address] = 0

    # Remake the shards, same algorithm as in the main function
    shard_id = 1
    for _ in range(0, int(shard_count)-1):
        shard_view[shard_id] = []
        shard_view[shard_id].append(view_list.pop(0))
        shard_view[shard_id].append(view_list.pop(0))
        if socket_address in shard_view[shard_id]:
            replica_shard_id = shard_id
            shard_member_list = shard_view[shard_id]
        shard_id += 1
    
    shard_view[shard_count] = view_list
    if socket_address in shard_view[shard_count]:
        replica_shard_id = shard_count
        shard_member_list = shard_view[shard_count]

    # Increment reshard counter
    reshard_count += 1

    # Send the new shards and vector clocks to all replicas 
    view_list = view.split(",")
    for address in view_list:
        if address != socket_address:
            try:
                value = {"new-shard-view":shard_view, "new-vector-clock":vector_clock, "new-reshard-count":reshard_count}
                requests.put("http://"+address+"/update-shard-and-vector-clock", json=value)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                pass


    for address in view_list:
        if address != socket_address:
            try:
                response = requests.get('http://'+address+'/get_store')
                key_value_store.update(response.json()['store'])
                requests.delete("http://"+address+"/delete_store")
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                pass
    
    key_value_store_copy = key_value_store.copy()
    for k,v in key_value_store.items():
        keyID = hash_mod_n(k)
        for member in shard_view[keyID]:
            if member != socket_address:
                value = {"value":v}
                requests.put("http://"+member+"/redistribute-keys/"+k, json=value)
        if keyID != replica_shard_id:
            del key_value_store_copy[k]

    key_value_store = key_value_store_copy
    
    return jsonify({"message":"Resharding done successfully"}), 200

@app.route('/redistribute-keys/<key>', methods=["PUT"])
def redistribute_keys(key):
    global key_value_store
    value_request = request.json
    key_value_store[key] = value_request["value"]
    return "done"


@app.route('/update-shard-and-vector-clock', methods=['PUT'])
def update_shard_and_vector_clock():
    global shard_view
    global vector_clock
    global replica_shard_id
    global shard_member_list
    global reshard_count

    get_most_up_to_date()
    value_request = request.json
    shard_view = value_request["new-shard-view"]
    shard_view = {int(k):[str(i) for i in v] for k,v in shard_view.items()}
    for shard_id in shard_view.keys():
        if socket_address in shard_view[shard_id]:
            replica_shard_id = shard_id
            shard_member_list = shard_view[shard_id]
    
    vector_clock = value_request["new-vector-clock"]
    vector_clock = {str(k):int(v) for k,v in vector_clock.items()}
    
    reshard_count = int(value_request["new-reshard-count"])
    return "done"

@app.route('/delete_store', methods=['DELETE'])
def delete_store():
    global key_value_store
    key_value_store = {}
    return "done"

if __name__ == "__main__":
    if shard_count:
        reshard_count = 0
        shard_count = int(shard_count)
        shard_id = 1
        view_list = view.split(",")
        for i in range(0, int(shard_count)-1):
            shard_view[shard_id] = []
            shard_view[shard_id].append(view_list.pop(0))
            shard_view[shard_id].append(view_list.pop(0))
            if socket_address in shard_view[shard_id]:
                replica_shard_id = shard_id
                shard_member_list = shard_view[shard_id]
            shard_id += 1
        
        shard_view[shard_count] = view_list
        if socket_address in shard_view[shard_count]:
            replica_shard_id = shard_count
            shard_member_list = shard_view[shard_count]

    for address in view.split(","):
        vector_clock[address] = 0

    background()
    app.run(host="0.0.0.0", port=8085, debug=True)