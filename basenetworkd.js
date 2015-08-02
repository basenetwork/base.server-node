#!/usr/bin/env node

/**
 * base.network NodeJS-server 0.1 (c) 2015 Denis Glazkov | https://github.com/basenetwork/
 *
 * http://base.network/
 *
 */

try {
    require("sqlite3");
} catch(e) {
    console.log("\n\n" +
    "\n========= ERROR: require('sqlite3') ===============" +
    "\n(error details: " + e + ")" +
    "\n" +
    "\nTry to install module from sources:" +
    "\n" +
    "\n       npm install sqlite3 --build-from-source" +
    "\n");
}

//------- start node-server -----------
require("./lib/").startServer(process.argv);
