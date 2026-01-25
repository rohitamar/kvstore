from flask import Flask, request, Response 
import json
import requests 
import sys 

from hashring.HashRing import HashRing

app = Flask(__name__)

HTTP_METHODS = ["GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"]

h = HashRing(100)

@app.route('/ping', methods=["GET"])
def ping():
    return Response(
        response=json.dumps({
            "message": "Pinged!"
        }),
        status=200,
        content_type="application/json"
    )

@app.route('/<path:path>', methods=HTTP_METHODS)
def entry(path):
    error_response = Response(
        response=json.dumps({
            "error": "Invalid path."
        }),
        status=404,
        content_type="application/json"
    )

    if request.method not in ["GET", "PUT"]:
        return error_response
    
    if request.method == 'PUT':
        if path != 'insert':
            return error_response
        
        data = request.get_json()
        key = data['key']

        host_cache = h.get_index(key)

        downstream_response = requests.request(
            method=request.method,
            url=f'{host_cache}/api/insert',
            json=data
        )

        return Response(
            status=downstream_response.status_code,
            response=downstream_response.content,
            headers=downstream_response.headers
        )
    
    if path.startswith('get'):
        path_params = path.split('/')
        if len(path_params) != 2:
            return error_response
        
        _, key = path_params
        host_cache = h.get_index(key)
        
        downstream_response = requests.request(
            method='GET',
            url=f'{host_cache}/api/get/{key}'
        )
        
        return Response(
            status=downstream_response.status_code,
            response=downstream_response.content,
            headers=downstream_response.headers
        )
    
    return error_response

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print('Need ports of cache instances')
        exit(1)
    
    ports = sys.argv[1:]
    for port in ports:
        url = f'http://127.0.0.1:{port}'
        h.add_node(url)
        print(f"Linked to {url}")
    app.run(port=8080, debug=True)