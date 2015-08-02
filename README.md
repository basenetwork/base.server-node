base.network server-node
========================
http://base.network

Install server node
-------------------
Server-node work on [Node.js](https://nodejs.org/)-technology. [Setup Node.js](http://howtonode.org/how-to-install-nodejs) on your server before installing.

### Linux
``` shell
git clone https://github.com/basenetwork/base.server-node

# install sqlite3 module from sources
cd base.server-node && npm install sqlite3 --build-from-source && cd ..

# start server
nohup base.server-node/start.js  >/var/log/base.network.log &
```

If sqlite3 module won’t build you’re probably missing one of the python or gcc dependencies, 
on linux try running `sudo npm install -g node-gyp`, 
`sudo apt-get install -y build-essential python-software-properties python g++`
make before retrying the build from source.


### Usage  
``` txt
base.server-node/start.js [options]
  OPTIONS:
    --host=<ip_addr> - IPv4 or IPv6 address. default: chose from network interfaces
    --port=<num>     - Port. default: 8080
    --dir=<path>     - Work directory. default: ~/.basenetwork/
    --size=<num>     - Storage-size. Allocate of <size> GiB
    --debug=<0|1>    - Out debug info to stdin
    --clear=<0|1>    - Clear storage data
```