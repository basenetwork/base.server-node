var fs              = require("fs");
var crypto          = require("crypto");
var http            = require('http');
var os              = require("os");
var zlib            = require('zlib');
var _               = require("./utils");
var HttpConnection  = require("./HttpConnection");
var Certificate     = require("./Certificate");
var log             = console.log;

var storageClasses = {
    D: require("./Storage/Data"),
    P: require("./Storage/Public"),
    F: require("./Storage/Files"),
    N: require("./Storage/Names")
};
var storageTypes = Object.keys(storageClasses);
var minSegSizeGiB = 2;
var defaultNodePort = 8080;

var _rePatternSeg = '[' + storageTypes.join('') + '][0-7]*';
var reSegment    = new RegExp('^' + _rePatternSeg + '$');
var reSegmentURL = new RegExp('^/-/' + _rePatternSeg + '/');
var reSegmentDir = new RegExp('^dat_' + _rePatternSeg + '$');

function _isValidSegment(seg) {
    return reSegment.test(seg);
}

module.exports = _.class({
    version: 1,

    options: {
        dir:   null,
        host:  null,
        port:  null,
        size:  '32'    //GiB
    },

    dir: null,  // server directory
    nid: null,  // current node ID

    // certificate
    cert: null,

    // segments
    segments: null, // segment by oct-name

    constructor: function(argv) {
        if(argv.indexOf('help') >= 0 || argv.indexOf('-?') >= 0) {
            this.showUsage();
            process.exit(0);
        }

        //process.on('uncaughtException', function (error) {
        //    console.log('\n\n--------------- Exception', error);//.stack);
        //});

        //-------- options ---------
        var options = _.ex(this.options, this.parseArguments(argv));
        var host = options.host || this.getExternalInterface() || this.fatal("Empty --host param");
        var port = options.port|0 || defaultNodePort;

        // current Node ID
        this.nid = host + '/' + port;

        //------ root dir -----------
        var dir = options.dir || (process.env.HOME || process.env.HOMEPATH || process.env.USERPROFILE || ".") + "/.basenetwork";
        this.dir = _.mkdir(dir);
        this.nodesInfoPath = this.dir + '/nodes.dat';

        this.log('Work dir: ' + this.dir);

        this.cert = Certificate.loadPrivate(this.dir + "/private.cert");
        this.allocSize = 0;
        this.usedSize = 0;
        this.totalAllocSizeGiB = (this.options.size|0) * _.GiB;

        if(this.totalAllocSizeGiB < minSegSizeGiB * _.GiB) this.fatal("Allocated space is too small. Size should be more than " + minSegSizeGiB + " GiB");

        this.initNodesInfo();

        // choose selected segments
        if(options.segments) {
            options.segments.split(',')
                .filter(_isValidSegment)
                .forEach(this.initSegment, this);
        }

        // read existed segments
        this.initExistedSegments();
        this.initNewSegments();

        this.log('Allocated size: ' + _.formatSize(this.allocSize));
        this.log('Real used size: ' + _.formatSize(this.usedSize) + ' ('+(this.usedSize/this.allocSize*100).toFixed(2)+'%)');

        this.startHttpServer();
        this.saveNodesInfo();
        this.refreshNodesInfo();
    },

    getNodesBySegment: function(seg, count) {
        var nodes = (this.nodesBySeg[seg] || []).slice(), res;

        if(!nodes.length) return [];
        if(!nodes._sorted) nodes._sorted = !!nodes.sort(_.compareIP);

        // find neighboring node
        for(var nid in nodes) {
            if(_.compareIP(nodes[nid], this.nid) > 0) {  //todo: fast search
                res = nodes.splice(nid, 1);
                break;
            }
        }

        res = res || nodes.splice(0, 1);

        // and select random nodes
        while(--count && nodes.length) {
            res.push(nodes.splice(nodes.length * Math.random() | 0, 1)[0]);
        }

        return res;
    },

    parseNodeID: function(nid) {
        if(!nid) return null;

        var a = nid.split('/');
        var host = a[0].trim().toLowerCase();
        var port = a[1]|0 || defaultNodePort;

        return /^([\d\.]{7,15}|[0-9a-f:]{3,39})$/.test(host) && {
            nid: host + '/' + port,
            host: host,
            port: port
        };
    },

    parseNodeHeader: function(conn) {
        var node = this.parseNodeID(conn._req.headers["x-base-node"]);

        return node
            && node.host
            && node.port
            && node.host.substr(0, 6) !== '127.0.'
            && node.host.substr(0, 8) !== '192.168.'
            //&& conn._req.connection.remoteAddress == node.host // check real IP
            && node.nid;
    },

    addNewNode: function(nid) {
        if(nid !== this.nid
            && this.parseNodeID(nid)
            && !this.nodes[nid] && !this.newNodes.get(nid) && (this.nodesErrors.get(nid)||0) < +new Date()) {

            this.newNodes.set(nid, {});
        }
    },

    addNode: function(node) {
        if(!node || !node.nid || node.nid === this.nid) return;
        var nid = node.nid;
        var _node = this.nodes[nid];

        // already added same version and segment type
        if(_node && _node.ver === node.ver && _node.seg === node.seg) return;
        if(_node) this.removeNode(nid);
        var segments = String(node.seg||'').split(',').filter(_isValidSegment);

        this.nodes[nid] = {
            ver: parseFloat(node.ver) || 0,
            seg: segments.join(',')
        };

        this._fChangedNodesInfo = true;
        this.newNodes.unset(nid);
        this.nodesErrors.unset(nid);

        segments.forEach(function(seg){
            var nodes = this.nodesBySeg[seg] || (this.nodesBySeg[seg] = []);

            if(nodes.indexOf(nid) < 0) {
                nodes._sorted = !nodes.push(nid);
            }
        }, this);
    },

    removeNode: function(nid, err) {
        if(this.nodes[nid]) {
            (this.nodes[nid].seg || '').split(',').forEach(function(seg) {
                var nodes = this.nodesBySeg[seg], i;
                if(nodes && (i = nodes.indexOf(nid)) >= 0) nodes.splice(i, 1);
            }, this);

            delete this.nodes[nid];
            this._fChangedNodesInfo = true;
        }

        this.newNodes.unset(nid);
        if(err) this.nodesErrors.set(nid, +new Date() + (1 + Math.random()) * 5e3); //todo:!!!!!! 600e3
    },

    requestToNode: function(op) {
        var nid = op.nid;
        var onData = op.onData;
        var onFinish = op.onFinish;
        var fAsync = op.async;
        var self = this;
        var queue = [];
        var lines = '';
        var fFinish, strm, response, fProcessing;

        if(fAsync && !onData) throw 'Empty onData';

        function fnAbort(err) {
            if(!queue) return;
            queue = null;

            self.debug('Request to ' + nid + ' ' + op.path + ' aborted! error: ' + err);
            self.removeNode(nid, true);

            onFinish && onFinish.call(self, err); // finish with error
            if(strm) strm.resume();
        }

        function fnNext(err) {
            if(!queue) return;
            if(err) return fnAbort(err);

            if(!queue.length) {
                if(fFinish) {  // success finish
                    return onFinish && onFinish.call(self);
                }
                fProcessing = false;
                return;
            }

            try {
                if(onData && queue.length) {
                    fProcessing = true;
                    onData.call(self, queue.shift());
                }
            } catch (e) {
                fnAbort(e);
            }
        }

        function processLine(line) {
            if(!queue || !line || !line.trim()) return;
            var pack = _.parseJSON(line);
            if(!pack) return fnAbort('Bad JSON');

            queue.push(pack);
            if(!fAsync || !fProcessing) fnNext();
        }

        var node = this.parseNodeID(nid);
        if(!node) {
            return fnAbort('Bad nid');
        }

        var headers = {
            host: '-',
            'Accept-Encoding': 'gzip'
        };

        if(this.segments) { // if server initialized segments
            headers['x-base-node'] = this.nid;
        }

        // todo: add header If-None-Match: hash(lastResponse(path))
        this.debug('HTTP GET '+node.host+':'+node.port+' '+ op.path);
        http.get({
            host: node.host,
            port: node.port,
            path: op.path,
            localAddress: this.nodeAddr || (this.nodeAddr = this.nid.split('/')[0]),
            headers: headers
        }, function(_response) {
            strm = response = _response;
            if(response.statusCode != 200) {
                return fnAbort('Status code ' + response.statusCode);
            }
            if(response.headers['content-encoding'] === 'gzip') {
                strm = response.pipe(zlib.createGunzip());
            }
            switch(op.type) {
                case 'json':
                    strm.on("data", function(chunk) {
                        if(!queue) return;
                        lines += chunk;
                        for(var i; queue && (i=lines.indexOf("\n")) >= 0; lines = lines.substr(i + 1))
                            processLine(lines.substr(0, i));
                        if(lines.length > 65 * _.MiB) return fnAbort('Limit is exceeded');
                    }).on("end", function() {
                        fFinish = true;
                        processLine(lines);
                        if(!fAsync || !fProcessing) fnNext(); // finish
                    });
                    break;

                case 'file':
                    var hash = crypto.createHash('sha256'), size = 0;
                    var ws = _.createTempStream(function(err) {
                        onFinish.call(self, err, !err && { tmpfile: ws._tmpfile, hash: hash.digest('hex'), size: size });
                    });
                    if(ws) strm.on('data', function(chunk){
                        size += chunk.length;
                        hash.update(chunk);
                    }).pipe(ws);
                    break;

                default:
                    throw 'Unknown type';
            }
        }).on('error', fnAbort);

        return {
            next: fnNext,
            abort: fnAbort,
            getResponseHeader: function(name) {
                return response && response.headers && response.headers[name]
            }
        };
    },

    //--------- utils ------------------
    log: function(msg) {
        console.log("[" + new Date().toISOString() + "]", msg);
    },

    debug: function(msg) {
        if(this.options.debug)
            console.log("debug [" + new Date().toISOString() + "]", msg);
    },

    isTestMode: function() {
        return !!this.options['test-mode'];
    },

    fatal: function(err) {
        throw err;
    },

    showUsage: function () {
        console.log([
            "usage: node basenetworkd [options]",
            "  OPTIONS:",
            "    --host=<ip_addr> - IPv4 or IPv6 address. By default: chose from network interfaces",
            "    --port=<num>     - Port. default: " + defaultNodePort,
            "    --dir=<path>     - Work directory. default: ~/.basenetwork/",
            "    --size=<num>     - Storage-size. Allocate of <size> GiB. default: " + this.options.size + ' GiB',
            "    --debug=<0|1>    - Out debug info to stdin",
            "    --clear=<0|1>    - Clear all storage data",
            ""
        ].join("\n"));
    },

    parseArguments: function (argv) {
        var args = {};

        for(var i in argv) {
            var param = argv[i], matches;

            if(matches = param.match(/^--([a-zA-Z\-]+)=(.*)/)) {
                args[matches[1]] = matches[2];

            } else if(matches = param.match(/^((\d+\.\d+\.\d+\.\d+)|\[([\d+\:]+)\])(:(\d+))?$/)) {  // ipv4:port [ipv6]:port
                args.host = matches[2] || matches[3];
                args.port = matches[5] || args.port;
            }
        }
        return args;
    },

    getExternalInterface: function () {
        var interfaces = os.networkInterfaces() || {}, host;

        for(var ifc in interfaces) {
            interfaces[ifc].forEach(function(int){
                if(!host && !int.internal && !/^(127\.0\.\d+\.\d+|::1|fe80(:1)?::1(%.*)?)$/.test(int.address)) {
                    host = int.address;
                }
            }, this);
            if(host) break;
        }

        this.log('Chosen IP-address: ' + host);
        return host;
    },

    //------------- segments ---------------------
    initNewSegments: function() {
        // choose random segments in each ring
        // start from first ring
        var ring = 1;
        while(this.initNewSegmentsInRing(ring)) {
            ring++;
        }
        // and try to add segments from lower rings (if enough space)
        for(--ring; ring>=0; ring--) {
            this.initNewSegmentsInRing(ring);
        }
    },

    initNewSegmentsInRing: function(ring) {
        var segments = []; // add all possible segments for this ring
        storageTypes.forEach(function(type) {
            // exclude N-storage for non zero ring
            if(type == 'N' && ring) return;

            var segmentsCount = 1 << ring * 3; // count segments in ring.  8^ring
            while(segmentsCount--) {
                var octNum = ring? ("0000000000000000"+segmentsCount.toString(8)).slice(-ring) : "";
                segments.push(type + octNum);
            }
        });
        // exclusion. for each ring try to add one of zero-ring segments
        segments.push(_.shuffle(storageTypes, 1)[0]);

        return _.shuffle(segments).reduce(function(res, seg) {
            return this.initSegment(seg) || res
        }.bind(this), false);
    },

    initExistedSegments: function() {
        fs.readdirSync(this.dir).forEach(function(fileName){
            if(reSegmentDir.test(fileName) && fs.statSync(this.dir + '/' + fileName).isDirectory()) {
                // filename-format is "dat_<seg>"
                this.initSegment(fileName.substr(4));
            }
        }, this);
    },

    initSegment: function (seg) {
        this.segments = this.segments || {};
        if(this.segments[seg]) return;

        var StorageClass = storageClasses[seg.substr(0,1)];
        var storage = new StorageClass(seg);

        if(this.allocSize + storage.getAllocatedSize() <= this.totalAllocSizeGiB) {
            this.segments[seg] = storage;
            storage.initStorage(this);
            this.allocSize += storage.getAllocatedSize();
            this.usedSize += storage.usedSize;
            this.log(' - init segment <' + seg + '>');
            return true;
        }
    },

    //--------- http ------------
    startHttpServer: function() {
        var node = this.parseNodeID(this.nid);
        this.log('Open http server (IP: ' + node.host + ' PORT: ' + node.port + ')');
        http.createServer(this.onWebRequest.bind(this)).listen(node.port, node.host);
    },

    onWebRequest: function(request, response) {
        var conn = new HttpConnection(request, response);
        var path = conn.urlPath;

        // --- check new node ---
        this.addNewNode(this.parseNodeHeader(conn));

        //---- add header ------
        conn.setResponseHeader('Access-Control-Allow-Origin', 'http://core.base.network');

        //--- GET|POST segment data   /-/<seg>/<command>...   ---
        if(reSegmentURL.test(path)) {
            var segment = this.segments && this.segments[conn.urlParts[2]];
            if(!segment) {
                return conn.response400('Segment not found');
            }
            try {
                return segment.processHttpRequest(conn);
            } catch (e) {
                return conn.response500(e);
            }

        } else if(conn.method === 'GET') {
            switch(path) {
                case '/':
                    return conn.response(200, 'MAIN PAGE');

                case '/-/about':
                    return this.httpAbout(conn);

                case '/-/nodes':
                    return this.httpNodes(conn);

                case '/favicon.ico':
                    return this.httpFavicon(conn);

                default:
            }
            return conn.response(404);
        }
        conn.response400();
    },

    httpAbout: function(conn) {
        if(!this._httpInfo || this._httpInfoTs < +new Date() - 5e3) {
            var segments = {};
            if(this.segments) {
                for(var i in this.segments) segments[i] = this.segments[i].getHttpInfo();
            }
            this._httpInfo = JSON.stringify({
                ver: this.version,
                nid: this.nid,
                segments: segments,
                updated: this._httpInfoTs = +new Date()
            });
        }

        conn.response(200, this._httpInfo);
    },

    httpNodes: function(conn) {
        conn.setResponseHeader('X-Remote-Addr', conn.getRemoteAddress(), true);
        conn.response(200, fs.createReadStream(this.nodesInfoPath));
    },

    httpFavicon: function(conn) {
        conn.setContentType('ico');
        conn.setExpire();
        conn.response(200, fs.createReadStream(__dirname + "/../data/favicon.ico"));
    },

    //--------- nodes ------------
    nodes: {/*
        "<ip4|ip6>/<port:int>": {
            seg: "<segment>|...",
            ver: <ver:int>,        // time of last request
        }
     */
    },
    nodesBySeg: {},
    newNodes: _.cache(500),
    nodesErrors: _.cache(500),

    initNodesInfo: function() {
        if(this.isTestMode() && !this.options.nodes) {
            // default nodes for testing
            this.options.nodes = '127.0.0.1:8080,127.0.0.1:8081';
        }
        this.loadNodesInfo(this.nodesInfoPath)                   // from saved file
         || this.loadNodesInfo(__dirname + '/../data/nodes.dat') // from init-file
         || this.fatal("Not found any nodes");
    },

    loadNodesInfo: function(filePath) {
        try {
            var cont = fs.readFileSync(filePath).toString();
            cont.split("\n").forEach(function(node){
                this.addNode(_.parseJSON(node));
            }, this);
        } catch(e) {
            return 0;
        }
        return _.size(this.nodes);
    },

    //periodically sync nodes-info on disk
    saveNodesInfo: function() {
        if(this.isTestMode()) return;

        var delay = function() {
            setTimeout(this.saveNodesInfo.bind(this), 3001);
        }.bind(this);

        if(!this._fChangedNodesInfo) {
            return delay();
        }
        this._fChangedNodesInfo = false;

        // write nodes info to file
        var file = fs.createWriteStream(this.nodesInfoPath);
        var localSegments = [];
        for(var seg in this.segments) {
            //if(this.segments[seg].isInitialized())
            localSegments.push(seg);
        }
        if(localSegments.length) {
            file.write(JSON.stringify({
                nid: this.nid,
                ver: this.version,
                seg: localSegments.join(",")
            }) + "\n", "binary");
        }
        for(var nid in this.nodes) {
            file.write(JSON.stringify(_.ex(_.obj("nid", nid), this.nodes[nid])) + "\n", "binary");
        }
        file.end();
        file.on('finish', delay);
    },

    refreshNodesInfo: function() {
        var delay = function() {
            setTimeout(this.refreshNodesInfo.bind(this), 3001);
        }.bind(this);

        var nid = Math.random() < 0.10 && this.nodesErrors.anyKey()
                || this.newNodes.anyKey()
                || _.randomKey(this.nodes);
        if(!nid || nid == this.nid) {
            return delay();
        }
        this.requestToNode({
            nid: nid,
            path: '/-/nodes',
            type: 'json',

            onData: function(data) {
                if(data
                && data.nid
                && data.ver && data.ver <= this.version
                && typeof data.seg === "string"
                && (data.seg = data.seg.split(",").filter(_isValidSegment).join(","))) {
                    if(data.nid === nid) this.addNode(data);
                    else this.addNewNode(data.nid);
                }
            }.bind(this),

            onFinish: delay
        });
    }
});
