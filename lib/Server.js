var fs      		= require("fs");
var crypto  		= require("crypto");
var http    		= require('http');
var os				= require("os");
var zlib 			= require('zlib');
var _ 				= require("./utils");
var HttpConnection	= require("./HttpConnection");
var Certificate		= require("./Certificate");
var log 			= console.log;

var availableSegmentTypes = {
	ring_0: ['N', 'D', 'P', 'F'],
	ring_default: ['D', 'P', 'F']
};

var StorageClasses = {
	D: require("./Storage/Data"),
	P: require("./Storage/Public"),
	F: require("./Storage/Files"),
	N: require("./Storage/Names")
};

var regexps = {
	segmentId: /'^[A-Z]+[0-7]*$/,
	segmentDir: /^dat_[DFNP][0-7]*$/,
	args: {
		simple: /^--([a-zA-Z\-]+)=(.*)/,
		networkAddr: /^((\d+\.\d+\.\d+\.\d+)|\[([\d+\:]+)\])(:(\d+))?$/ // ipv4:port [ipv6]:port
	},
	localHost: /^(127\.0\.\d+\.\d+|::1|fe80(:1)?::1(%.*)?)$/
};

var minSegSizeGB = 2;

var defaultNodePort = 8080;

module.exports = _.class({

	version: 1,

	options: {
		dir:	null,
		host:	null,
		port: 	defaultNodePort,
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
			this.showHelpMessage();
			process.exit(0);
		}

		//process.on('uncaughtException', function (error) {
		//	console.log('\n\n--------------- Exception', error);//.stack);
		//});

		//-------- options ---------
		var options = this.fillOptionsFromArgs(this.options, argv);
		var host = options.host;
		var port = options.port|0 || defaultNodePort;
		if(!host) {
			host = this.tryToSearchHost();
		}
		host || this.fatal("Empty --host param");

		this.nodeId = host + '/' + port;

		//------ root dir -----------
		var dir = options.dir || (process.env.HOME || process.env.HOMEPATH || process.env.USERPROFILE || ".") + "/.basenetwork";
		this.dir = _.mkdir(dir);
		this.nodesInfoPath = this.dir + '/nodes.dat';
		this.nodesInfoLocations = [
			this.nodesInfoPath,
			__dirname + '/../data/nodes.dat'
		];

		this.log('Work dir: ' + this.dir);

		this.cert = Certificate.loadPrivate(this.dir + "/private.cert");
		this.allocSize = 0;
		this.usedSize = 0;
		this.totalAllocSizeGiB = (this.options.size|0) * _.GiB;

		if(this.totalAllocSizeGiB < minSegSizeGB * _.GiB) this.fatal("Allocated space is too small. Size should be more than " + minSegSizeGB + "GiB");

		//--- init nodes info ---
		this.initNodesInfo();

		// choose selected segments
		if(options.segments !== undefined) {
			options.segments.split(',').forEach(function(seg){
				if(/^[DFNP][0-7]*$/.test(seg)) this.initSegment(seg);
			}, this);
		}

		// read existed segments
		this.loadSegmentsFromDir(dir);
		this.initRings();

		this.log('Allocated size: ' + _.formatSize(this.allocSize));
		this.log('Real used size: ' + _.formatSize(this.usedSize) + ' ('+(this.usedSize/this.allocSize*100).toFixed(2)+'%)');

		this.startHttpServer();
		this.tryToFsyncNodesInfo(true);
		this.tryToScanNodes();
	},

	showHelpMessage: function () {
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
	},

	fillOptionsFromArgs: function (options, argv) {
		for(var i in argv) {
			var param = argv[i], matches;

			if(matches = param.match(regexps.args.simple)) {
				options[matches[1]] = matches[2];

			} else if(matches = param.match(regexps.args.networkAddr)) {
				options.host = matches[2] || matches[3];
				options.port = matches[5] || options.port;
			}
		}

		return options;
	},

	tryToSearchHost: function () {
		var interfaces = os.networkInterfaces() || {}, host;

		for(var ifc in interfaces) {
			interfaces[ifc].forEach(function(int){
				if(!host && !int.internal && !regexps.localHost.test(int.address)) {
					host = int.address;
				}
			}, this);
			if(host) break;
		}

		this.log('Chosen IP-address: ' + host);
		return host;
	},

	initRings: function () {
		// choose suitable segments
		// TODO: after load nodes-info choose unprocessed segments and segments with low count of peers
		for(var ringNumber = 1; ; ringNumber++) {
			if(!this.initRing(ringNumber)) break;
		}

		// and try to add segments from zero ring
		_.shuffle(this.getAvailableSegmentTypes(0)).forEach(this.initSegment, this);
	},

	initRing: function (ringNumber) {
		// for first ring try to add one of zero-ring segment
		var segments = ringNumber == 1 ? _.shuffle(this.getAvailableSegmentTypes(0), 1) : [];
		var ringInfo = this.getRingInfo(ringNumber);

		ringInfo.segmentTypes.forEach(function(segmentType){
			var segmentsCount = ringInfo.segmentsCount;
			while(segmentsCount--) segments.push(segmentType+("0000000000000000"+segmentsCount.toString(8)).slice(-ringNumber));
		});

		return _.shuffle(segments).reduce(function(res, seg) {
			return this.initSegment(seg) || res
		}.bind(this), false);
	},

	loadSegmentsFromDir: function (dir) {
		fs.readdirSync(dir).forEach(function(fileName){
			if(regexps.segmentDir.test(fileName) && fs.statSync(dir + '/' + fileName).isDirectory()) {
				this.initSegment(fileName.substr(4));
			}
		}, this);
	},

	initSegment: function (segmentId) {
		this.segments = this.segments || {};
		if(this.segments[segmentId]) return true;

		var StorageClass = StorageClasses[segmentId.substr(0,1)];
		var storage = new StorageClass(segmentId);

		if(this.allocSize + storage.getAllocatedSize() <= this.totalAllocSizeGiB) {
			this.segments[segmentId] = storage;
			storage.initStorage(this);
			this.allocSize += storage.getAllocatedSize();
			this.usedSize += storage.usedSize;
			this.log(' - init segment <' + segmentId + '>');
			return true;
		}
	},

	getRingInfo: function (ringNumber) {
		return {
			segmentTypes: this.getAvailableSegmentTypes(ringNumber),
			segmentsCount: this.getSegmentsCount(ringNumber)
		}
	},

	getAvailableSegmentTypes: function(ringNumber) {
		return availableSegmentTypes["ring_" + ringNumber] || availableSegmentTypes["ring_default"];
	},

	getSegmentsCount: function (ringNumber) {
		return 1 << ringNumber * 3; // count segments in ring
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
				nodeId: this.nodeId,
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
			this.options.nodes.replace(/\:/g, '/').split(/[\s,;]+/).forEach(function(nodeId){
				this.addNode({nodeId: nodeId});
			}.bind(this));
			return;
		}

		this.tryToLoadNodesInfo() || this.fatal("Can`t find any nodes");
	},

	tryToLoadNodesInfo: function () {
		return this.nodesInfoLocations.some(function (nodesFilePath) {
			return this.loadNodesInfo(nodesFilePath);
		}, this);
	},

	loadNodesInfo: function(filePath) {
		try {
			fs.readFileSync(filePath).toString().split("\n").forEach(function(node){
				this.addNode(_.parseJSON(node));
			}, this);

		} catch(e) {
			return 0;
		}

		return _.size(this.nodes);
	},

	getNodesBySegment: function(segmentId, count) {
		var nodes = (this.nodesBySeg[segmentId] || []).slice(), res;

		if(!nodes.length) return [];
		if(!nodes._sorted) nodes._sorted = !!nodes.sort(_.compareIP);

		// find neighboring node
		for(var nodeId in nodes) {
			if(_.compareIP(nodes[nodeId], this.nodeId) > 0) {  //todo: fast search
				res = nodes.splice(nodeId, 1);
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

	parseNodeId: function(nodeId) {
		if(!nodeId) return null;

		var a = nodeId.split('/');
		var host = a[0].trim().toLowerCase();
		var port = a[1]|0 || defaultNodePort;

		return /^([\d\.]{7,15}|[0-9a-f:]{3,39})$/.test(host) && {
			id: host + '/' + port,
			host: host,
			port: port
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
				&& node.id
	},

	addNewNode: function(nodeId) {
		if(nodeId !== this.nodeId
			&& this.parseNodeId(nodeId)
			&& !this.nodes[nodeId] && !this.newNodes.get(nodeId) && (this.nodesErrors.get(nodeId)||0) < +new Date()) {
			this.newNodes.set(nodeId, {});
		}
	},

	addNode: function(node) {
		if(!node || !node.id || node.id === this.nodeId) return;
		var nodeId = node.id;
		var _node = this.nodes[nodeId];

		// already added same version and segment type
		if(_node && _node.ver === node.ver && _node.seg === node.seg) return;
		if(_node) this.removeNode(nodeId);
		var isSegmentId = function(str){ return regexps.segmentId.test(str); };
		var segments = String(node.seg||'').split(',').filter(isSegmentId);

		this.nodes[nodeId] = {
			ver: parseFloat(node.ver) || 0,
			seg: segments.join(',')
		};

		this._fsyncNodes = true;
		this.newNodes.unset(nodeId);
		this.nodesErrors.unset(nodeId);

		segments.forEach(function(segmentId){
			var nodes = this.nodesBySeg[segmentId] || (this.nodesBySeg[segmentId] = []);

			if(nodes.indexOf(nodeId) < 0) {
				nodes._sorted = !nodes.push(nodeId);
			}
		}, this);
	},

	removeNode: function(nodeId, err) {
		if(this.nodes[nodeId]) {
			(this.nodes[nodeId].seg || '').split(',').forEach(function(segmentId) {
				var nodes = this.nodesBySeg[segmentId], i;
				if(nodes && (i = nodes.indexOf(nodeId)) >= 0) nodes.splice(i, 1);
			}, this);

			delete this.nodes[nodeId];
			this._fsyncNodes = true;
		}

		this.newNodes.unset(nodeId);
		if(err) this.nodesErrors.set(nodeId, +new Date() + (1 + Math.random()) * 5e3); //todo:!!!!!! 600e3
	},

	//periodically sync nodes-info on disk
	tryToFsyncNodesInfo: function(start) {
		if(this.isTestMode()) return;

		var nextTry = this.tryToFsyncNodesInfo.bind(this),
			delayNextTry = function () {
				setTimeout(nextTry, 3001);
			};

		if(this._fsyncNodes || start) {
			this.fsyncNodesInfo(delayNextTry);

		} else {
			delayNextTry();
		}
	},

	fsyncNodesInfo: function (onFinish) {
		var file = fs.createWriteStream(this.nodesInfoPath);
		var localSegments = [];

		for(var seg in this.segments) {
			//if(this.segments[seg].isInitialized())
			localSegments.push(seg);
		}

		if(localSegments.length) {
			file.write(JSON.stringify({
				id: this.nodeId,
				ver: this.version,
				seg: localSegments.join(",")
			}) + "\n", "binary");
		}

		for(var nodeId in this.nodes) {
			file.write(JSON.stringify(_.ex(_.obj("nodeId", nodeId), this.nodes[nodeId])) + "\n", "binary");
		}

		file.end();
		file.on('finish', onFinish);
	},

	requestToNode: function(op) {
		var nodeId = op.nodeId,
			onData = op.onData,
			onFinish = op.onFinish,
			fAsync = op.async;

		var self = this,
			queue = [],
			lines = '',
			fFinish, strm, response, fProcessing;

		if(fAsync && !onData) throw 'Empty onData';

		function fnAbort(err) {
			if(!queue) return;
			queue = null;

			self.debug('Request to ' + nodeId + ' ' + op.path + ' aborted! error: ' + err);
			self.removeNode(nodeId, true);

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

		var node = this.parseNodeId(nodeId);
		if(!node) {
			return fnAbort('Bad nodeId');
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

	tryToScanNodes: function() {
		var nodeId = this.newNodes.anyKey() || _.randomKey(this.nodes),
			nextTry = this.tryToScanNodes.bind(this),
			delayNextTry = function () {
				setTimeout(nextTry, 3001);
			};

		if(!nodeId || nodeId == this.nodeId) {
			delayNextTry();
		} else {
			this.scanNodes(nodeId, delayNextTry);
		}
	},

	scanNodes: function (nodeId, onFinish) {
		this.requestToNode({
			nodeId: nodeId,
			path: '/-/nodes',
			type: 'json',

			onData: function(data) {
				if(data && data.nodeId && data.ver && data.ver <= this.version
				&& typeof data.seg === "string"
				&& (data.seg = data.seg.split(",").filter(function(seg) { return /^[A-Z]+\d*/.test(seg) }).join(","))) {
					if(data.nodeId === nodeId) this.addNode(data);
					else this.addNewNode(data.nodeId);
				}
			}.bind(this),

			onFinish: onFinish
		});
	}
});
