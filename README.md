base.network server-node
========================
http://base.network

Install server node
-------------------
### Linux
```
git clone https://github.com/basenetwork/base.server-node

# install sqlite3 module from sources
cd base.server-node && npm install sqlite3 --build-from-source && cd ..

# start server
base.server-node/start.js  >/var/log/base.network.log &
```


### Usage  
```
base.server-node/start.js [options]
  OPTIONS:
    --host=<ip_addr> - IPv4 or IPv6 address. default: chose from network interfaces
    --port=<num>     - Port. default: 8080
    --dir=<path>     - Work directory. default: ~/.basenetwork/
    --size=<num>     - Storage-size. Allocate of <size> GiB
    --debug=<0|1>    - Out debug info to stdin
    --clear=<0|1>    - Clear storage data
```