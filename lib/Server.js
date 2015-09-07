var fs      		= require("fs");
var crypto  		= require("crypto");
var http    		= require('http');
var os				= require("os");
var zlib 			= require('zlib');
var _ 				= require("./utils");
var HttpConnection	= require("./HttpConnection");
var Certificate		= require("./Certificate");
var log 			= console.log;

var defaultNodePort = 8080;

module.exports = _.class({

	version: 1,

	options: {
		dir:	null,
		host:	null,
		port: 	null,
		size:	'32'	//GiB
	},

	dir: null,
	nodeId: null,

	// certificate
	cert: null,

	// segments
	segments: null, // segment by oct-name

	constructor: function(argv) {

		if(argv.indexOf('help') >= 0 || argv.indexOf('-?') >= 0) {
			console.log([
				"usage: node basenetworkd [options]",
				"  OPTIONS:",
				"    --host=<ip_addr> - IPv4 or IPv6 address. By default: chose from network interfaces",
				"    --port=<num>     - Port. default: 8080",
				"    --dir=<path>     - Work directory. default: ~/.basenetwork/",
				"    --size=<num>     - Storage-size. Allocate of <size> GiB. default: " + this.options.size + ' GiB',
				"    --debug=<0|1>    - Out debug info to stdin",
				"    --clear=<0|1>    - Clear all storage data",
				""
			].join("\n"));
			process.exit(0);
		}
		//process.on('uncaughtException', function (error) {
		//	console.log('\n\n--------------- Exception', error);//.stack);
		//});

		//-------- options ---------
		var options = this.options;
		for(var i in argv) {
			var param = argv[i], matches;
			if(matches = param.match(/^--([a-zA-Z\-]+)=(.*)/)) {
				options[matches[1]] = matches[2];

			} else if(matches = param.match(/^((\d+\.\d+\.\d+\.\d+)|\[([\d+\:]+)\])(:(\d+))?$/)) { // ipv4:port [ipv6]:port
				options.host = matches[2] || matches[3];
				options.port = matches[5] || options.port;
			}
		}
		var host = options.host;
		var port = options.port|0 || defaultNodePort;
		if(!host) {
			var interfaces = os.networkInterfaces() || {};
			for(var ifc in interfaces)
				interfaces[ifc].forEach(function(int){
					if(!host && !int.internal && !/^(127\.0\.\d+\.\d+|::1|fe80(:1)?::1(%.*)?)$/.test(int.address)) {
						host = int.address;
					}
				}.bind(this));
			this.log('Chosen IP-address: ' + host);
		}
		host || this.fatal("Empty --host param");
		this.nodeId = host + '/' + port;

		//------ root dir -----------
		var dir = options.dir || (process.env.HOME || process.env.HOMEPATH || process.env.USERPROFILE || ".") + "/.basenetwork";
		this.dir = _.mkdir(dir);
		this.log('Work dir: ' + this.dir);

		this.cert = Certificate.loadPrivate(this.dir + "/private.cert");

		//--- init nodes info ---
		this.initNodesInfo();

		//----- start web server -------
		this.startHttpServer();

		//--- init segments -----
		var storageClasses = {
			D: require("./Storage/Data"),
			P: require("./Storage/Public"),
			F: require("./Storage/Files"),
			N: require("./Storage/Names")
		};
        var minSegSizeGB = 2; //(GiB) (size of D and P segments in zero-ring)
		var totalAllocSizeGiB = (this.options.size|0) * _.GiB;
		if(totalAllocSizeGiB < minSegSizeGB * _.GiB) this.fatal("Allocated space is too small. Size should be more then "+minSegSizeGB+"GiB");
		var allocSize = 0;
		var usedSize = 0;
        var initSegment = function(seg) {
			this.segments = this.segments || {};
            if(this.segments[seg]) return true;
            var cls = storageClasses[seg.substr(0,1)];
            var storage = new cls(seg);
            if(allocSize + storage.getAllocatedSize() <= totalAllocSizeGiB) {
				this.segments[seg] = storage;
                storage.initStorage(this);
				allocSize += storage.getAllocatedSize();
				usedSize += storage.usedSize;
				this.log(' - init segment <'+seg+'>');
                return true;
			}
		}.bind(this);

		// choose selected segments
		if(options.segments !== undefined) {
			options.segments.split(',').forEach(function(seg){
				if(/^[DFNP][0-7]*$/.test(seg))
					initSegment(seg);
			});
		}
		// read existed segments
		fs.readdirSync(dir).forEach(function(fileName){
			if(/^dat_[DFNP][0-7]*$/.test(fileName) && fs.statSync(dir+'/'+fileName).isDirectory()) {
				initSegment(fileName.substr(4));
			}
		});
		// choose suitable segments
        // TODO: after load nodes-info choose unprocessed segments and segments with low count of peers
        for(var ring = 1; ; ring++) {
            var segments = [];
            if(ring == 1) segments = _.shuffle(['N', 'D', 'P', 'F'], 1); // add random segment from zero-ring
            ['D', 'P', 'F'].forEach(function(type){
                var count = 1 << ring * 3; // count segments in ring
                while(count--) segments.push(type+("0000000000000000"+count.toString(8)).slice(-ring));
            });
            if(!_.shuffle(segments).reduce(function(res, seg){
                return initSegment(seg) || res
            }, false)) break;
        }
        // ... and add segments from zero ring
        _.shuffle(['N', 'D', 'P', 'F']).forEach(initSegment);

        this.log('Allocated size: ' + _.formatSize(allocSize));
		this.log('Real used size: ' + _.formatSize(usedSize) + ' ('+(usedSize/allocSize*100).toFixed(2)+'%)');

		this.fsyncNodesInfo(true);

		//--- start nodes scanner
		this.scanNodes();
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

	//--------- http ------------
	startHttpServer: function() {
		var node = this.parseNodeId(this.nodeId);
		this.log('Open http server (IP: ' + node.host + ' PORT: ' + node.port + ')');
		http.createServer(this.onWebRequest.bind(this)).listen(node.port, node.host);
	},
	
	onWebRequest: function(request, response) {
		var conn = new HttpConnection(request, response);
		var path = conn.urlPath;

		// --- check new node ---
		this.addNewNode(this.parseNodeHeader(conn));

		//---- add header ------
		if(this.isTestMode()) {
			conn.setResponseHeader('Access-Control-Allow-Origin', '*');
		} else {
			conn.setResponseHeader('Access-Control-Allow-Origin', 'http://core.base.network');
		}

		//--- GET|POST segment data   /-/<segment_num>/<segment_command>...   ---
		if(/^\/-\/[DFNP][0-7]*\//.test(path)) {
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
					return this.getHttpAbout(conn);

				case '/-/nodes':
					return this.getHttpNodes(conn);

				case '/favicon.ico':
					return this.getFavicon(conn);

				default:
			}
			return conn.response(404);
		}
		conn.response400();
	},

	getHttpAbout: function(conn) {
		if(!this._httpInfo || this._httpInfoTs < +new Date() - 5e3) {
			var segments = {};
			if(this.segments) {
				for(var i in this.segments) segments[i] = this.segments[i].getHttpInfo();
			}
			this._httpInfo = JSON.stringify({
				ver: this.version,
				nid: this.nodeId,
				segments: segments,
				updated: this._httpInfoTs = +new Date()
			});
		}
		conn.response(200, this._httpInfo);
	},

	getHttpNodes: function(conn) {
		conn.setResponseHeader('X-Remote-Addr', conn.getRemoteAddress(), true);
		conn.response(200, fs.createReadStream(this.dir + '/nodes.dat'));
	},

	getFavicon: function(conn) {
        conn.setContentType('ico');
        conn.setExpire();
        conn.response(200, fs.createReadStream(__dirname + "/../data/favicon.ico"));
	},

	//--------- nodes ------------
	nodes: {/*
		"<ip4|ip6>/<port:int>": {
			seg: "<segment>|...",
			ver: <ver:int>,		// time of last request
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
		if(this.options.nodes) {
			this.options.nodes.replace(/\:/g, '/').split(/[\s,;]+/).forEach(function(nid){
				this.addNode({nid: nid});
			}.bind(this));
			return;
		}
		var load = function(filepath) {
			try {
				fs.readFileSync(filepath).toString().split("\n").forEach(function(node){
					this.addNode(_.parseJSON(node));
				}.bind(this));
			} catch(e) {}
			return _.size(this.nodes);
		}.bind(this);
		load(this.dir + '/nodes.dat') || load(__dirname + '/../data/nodes.dat') || this.fatal("Can`t find any nodes");
	},

	getNodesBySegment: function(seg, count) {
		var i, res, arr = this.nodesBySeg[seg];
		if(!arr || !arr.length) return [];
		if(!arr._sorted) arr._sorted = !!arr.sort(_.compareIP);
		arr = arr.slice();
		for(i in arr) // find neighboring node
			if(_.compareIP(arr[i], this.nodeId) > 0) {  //todo: fast search
				res = arr.splice(i, 1);
				break;
			}
		res || (res = arr.splice(0, 1));
		while(--count && arr.length) // and select random nodes
			res.push(arr.splice(arr.length * Math.random() | 0, 1)[0]);
		return res;
	},

	parseNodeId: function(nid) {
		if(!nid) return null;
		var a = nid.split('/');
		var h = a[0].trim().toLowerCase();
		var p = a[1]|0 || defaultNodePort;
		return /^([\d\.]{7,15}|[0-9a-f:]{3,39})$/.test(h) && {
			nid: h + '/' + p,
			host: h,
			port: p
		}
	},

	parseNodeHeader: function(conn) {
		var node = this.parseNodeId(conn._req.headers["x-base-node"]);
		return node
            && node.host
            && node.port
            && node.host.substr(0, 6) !== '127.0.'
            && node.host.substr(0, 8) !== '192.168.'
            //&& conn._req.connection.remoteAddress == node.host // check real IP
            && node.nid
	},

	addNewNode: function(nid) {
        if(nid !== this.nodeId
		&& this.parseNodeId(nid)
		&& !this.nodes[nid] && !this.newNodes.get(nid) && (this.nodesErrors.get(nid)||0) < +new Date()) {
			this.newNodes.set(nid, {});
		}
	},

	addNode: function(node) {
		if(!node) return;
		var nid = node.nid;
		if(!nid || nid == this.nodeId) return;
		var _node = this.nodes[nid];
		if(_node && _node.ver === node.ver && _node.seg === node.seg) return;
		if(_node) this.removeNode(nid);
		var segments = String(node.seg||'').split(',').filter(function(seg){ return /^[A-Z]+[0-7]*$/.test(seg) });
		this.nodes[nid] = {
			ver: parseFloat(node.ver) || 0,
			seg: segments.join(',')
		};
		this._fsyncNodes = true;
		this.newNodes.unset(nid);
		this.nodesErrors.unset(nid);
		segments.forEach(function(seg){
			var arr = this[seg] || (this[seg] = []);
			if(arr.indexOf(nid) < 0) arr._sorted = !arr.push(nid);
		}.bind(this.nodesBySeg));
	},

	removeNode: function(nid, err) {
		if(this.nodes[nid]) {
			(this.nodes[nid].seg||'').split(',').forEach(function(seg) {
				var arr = this[seg], i;
				if(arr && (i = arr.indexOf(nid)) >= 0) arr.splice(i, 1);
			}.bind(this.nodesBySeg));
			delete this.nodes[nid];
			this._fsyncNodes = true;
		}
		this.newNodes.unset(nid);
		if(err) this.nodesErrors.set(nid, +new Date() + (1 + Math.random()) * 5e3); //todo:!!!!!! 600e3
	},

	//periodically sync nodes-info on disk
	fsyncNodesInfo: function(start) {
		if(this.isTestMode()) return;
		if(this._fsyncNodes || start) {
			var file = fs.createWriteStream(this.dir + '/nodes.dat');
			var localSegments = [];
			for(var seg in this.segments) {
				if(this.segments[seg].isInitialized())
					localSegments.push(seg);
			}
			if(localSegments.length) {
				file.write(JSON.stringify({
					nid: this.nodeId,
					ver: this.version,
					seg: localSegments.join(",")
				}) + "\n", "binary");
			}
			for(var nid in this.nodes)
				file.write(JSON.stringify(_.ex(_.obj("nid", nid), this.nodes[nid])) + "\n", "binary");
			file.end();
			file.on('finish', function() {
				setTimeout(this.fsyncNodesInfo.bind(this), 3001);
			}.bind(this));
		} else {
			setTimeout(this.fsyncNodesInfo.bind(this), 3001);
		}
	},

	requestToNode: function(op) {
		var nid = op.nid, onData = op.onData, onFinish = op.onFinish, fAsync = op.async;
		var self = this, queue = [], fFinish, strm, response, lines = '', fProcessing;
		if(fAsync && !onData) throw 'Empty onData';
		function fnAbort(err) {
			if(!queue) return;
			queue = null;
			self.debug('Request to ' + nid + ' ' +op.path+ ' aborted! error: ' + err);
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
		var node = this.parseNodeId(nid);
		if(!node) {
			return fnAbort('Bad nid');
		}
		var headers = {
			host: '-',
			'Accept-Encoding': 'gzip'
		};
		if(this.segments) { // if server initialized segments
			headers['x-base-node'] = this.nodeId;
		}
		// todo: add header If-None-Match: hash(lastResponse(path))
        this.debug('HTTP GET '+node.host+':'+node.port+' '+ op.path);
		http.get({
			host: node.host,
			port: node.port,
			path: op.path,
			localAddress: this.nodeAddr || (this.nodeAddr = this.nodeId.split('/')[0]),
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
					throw 'Unknow type';
			}
		}).on('error', fnAbort);
		return {
			next: fnNext,
			abort: fnAbort,
			getResponseHeader: function(name) {
				return response && response.headers && response.headers[name]
			}
		}
	},

	scanNodes: function() {
		var nid = this.newNodes.anyKey() || _.randomKey(this.nodes);
		if(!nid || nid == this.nodeId) {
			return setTimeout(this.scanNodes.bind(this), 3001);
		}
		this.requestToNode({
			nid: nid,
			path: '/-/nodes',
			type: 'json',
			onData: function(data) {
				if(data && data.nid && data.ver && data.ver <= this.version
				&& typeof data.seg === "string"
				&& (data.seg = data.seg.split(",").filter(function(seg) { return /^[A-Z]+\d*/.test(seg) }).join(","))) {
					if(data.nid === nid) this.addNode(data);
					else this.addNewNode(data.nid);
				}
			}.bind(this),
			onFinish: function() {
				setTimeout(this.scanNodes.bind(this), 3001);
			}.bind(this)
		});
	}

});
