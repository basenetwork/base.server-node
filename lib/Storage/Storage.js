var fs          = require("fs");
var sqlite3     = require('sqlite3');//.verbose();
var _		    = require("../utils");
var Certificate	= require("../Certificate");
var log 	    = console.log;

module.exports = _.class({

    server:  null,	// server object
    seg: 	 null,  // segment name 'D<num:oct>'
    ring:    null,	// ring of segment (int)
    num:     null,	// number of segment (oct)
    cert:    null,  // certificate of data-source
    ds:      null,  // unique id of current data-source  := certificate.hash():str20
    info:	 null,	// data source info (last sync time, etc.)
    sources: null,  // { "ds": { cert:<Certificate>, seq:<seq>, _seq:<seq> },...  }
    listeners: null, // { "uid": { "connectionID": <Response>,...}}
    usedSize: null,
    _initialized: false,

    /**
     * @param name      - string "A<segment_num:oct>"
     */
    constructor: function(name) {
        var sn = name.match(/^[A-Z]+([0-7]*)$/);
        if(!sn) throw "Incorrect segment name";

        this.seg = name;	// A<oct>
        this.num = sn[1];   // <oct>
        this.ring = this.num.length;
    },

    /**
     * @param server    - Server object
     */
    initStorage: function(server) {
        this.server = server;

        //------ init dir -------
        var dir = _.mkdir(server.dir + '/dat_' + this.seg);
        if(server.options.clear|0) {
            fs.readdirSync(dir).forEach(function(file){
                fs.unlinkSync(file = dir + '/' + file);
                server.log('- removed ' + file);
            });
        }
        //-----------------
        this.dir = dir;
        this.sources = {};
        this.cert = server.cert;
        this.ds = this.cert.getID();
        this.listeners = {}; // {uid: {connID: conn,...}}

        //--- init storage info ---
        this.info = fs.existsSync(dir + '/info.json') && _.parseJSON(fs.readFileSync(dir + '/info.json')) || {};

        //---- init db ----
        this.initDB(function(){
            // start full synchronization. todo: use special process??
            setTimeout(this.fullSynchronization.bind(this, true), 1009);
            this._initialized = true;
        }.bind(this));

        this.calcUsedSizeSync();
        setInterval(this.calcUsedSizeAsync.bind(this), 73009);
    },

    //-------- private methods ---------
    saveInfo: function(inf) {
        fs.writeFile(this.dir + '/info.json', JSON.stringify(_.ex(this.info, inf)));
    },

    checkUid: function(uid) {
        return typeof uid === "string"
            && /^[0-9a-f]{64}$/.test(uid)
            && (!this.num || this.num === parseInt(uid.substr(0,12), 16).toString(8).substr(0, this.ring));
    },

    checkCertID: function(certID) {
        return /^[0-9a-zA-Z\-_]{20}$/.test(certID);
    },

    checkDs: function(ds) {
        return this.checkCertID(ds);
    },

    parseCertificate: function(cert64) {
        return Certificate.parsePublic(cert64);
    },

    saveCertificate: function(cert) {
        // save author certificate to db
        var seg = this.seg;
        cert._saved && cert._saved[seg] || this._sql.insertAuthor.run({
            $aid:  cert.getID(),
            $cert: cert.toBuffer()
        }, function(err) {
            (cert._saved || (cert._saved = {}))[seg] = !err;
        });
    },

    calcUsedSizeSync: function() {
        this.usedSize = fs.readdirSync(this.dir).reduce(function(s, file) {
            return s + fs.statSync(this.dir + '/' + file).size;
        }.bind(this), 0);
    },

    calcUsedSizeAsync: function() {
        var fileStat = function(files, sum) {
            if(!files.length) return this.usedSize = sum;
            fs.stat(this.dir + '/' + files.shift(), function(err, st){
                fileStat(files, sum + (st && !st.isDirectory() && st.size|0));
            });
        }.bind(this);
        fs.readdir(this.dir, function(err, files) {
            if(files) fileStat(files, 0);
        });
    },

    isInitialized: function() {
        return this._initialized && this.usedSize !== null;
    },

    //---------- Abstract -------------
    getRingSize:        function()              { throw 'Abstract method' },    // count of segments in ring
    getAllocatedSize:   function()              { throw 'Abstract method' },
    getMaxPostSize:     function()              { throw 'Abstract method' },
    createDataTables:   function()              { throw 'Abstract method' },
    prepareData:        function(post, src, nid){ throw 'Abstract method' },


    //---------- DB ---------------
    db: null,
    _sql: null,

    prepareSQL: function(sqlObjs) {
        for(var i in sqlObjs) {
            //this.server.debug('- SQL-preparing: ' + sqlObjs[i]);
            var sql = sqlObjs[i], stm = this.db.prepare(sql);
            stm._vars = {};
            sql.replace(/\$[a-z][a-zA-Z0-9_]*/g, function(v){
                stm._vars[v] = true;
            });
            stm._filterParams = function(params) {
                var res = {};
                for(var i in this._vars) res[i] = params[i];
                return res;
            };
            (this._sql || (this._sql = {}))[i] = stm;
        }
    },

    initDB: function(fn) {
        this.db = new sqlite3.Database(this.dir + '/data.db');
        if(this.server.options.debug|0) this.db.on('trace', log);

        this.db.exec(
            // ------ data sources ---------
            "CREATE TABLE IF NOT EXISTS authors (		" + // all authors
            "	aid         TEXT    NOT NULL,			" + // unique author-id. char20
            "	cert        BLOB    NOT NULL,			" + // author certificate: (ver|public key|registrar-sign)
            "	size        INT     NOT NULL DEFAULT 0, " + //
            "	PRIMARY KEY(aid)                        " +
            "); " +

            // ------ data sources ---------
            "CREATE TABLE IF NOT EXISTS sources (		" + // all data sources (nodes) in segment
            "	ts          TIMESTAMP NOT NULL,         " + //
            "	ds          TEXT    NOT NULL,			" + // unique name of data source. char20
            "	cert        BLOB    NOT NULL,			" + // data source certificate
            "	seq         INT     NOT NULL DEFAULT 0, " + // last sequence in data source
            "	size        INT     NOT NULL DEFAULT 0, " + //
            "	PRIMARY KEY(ds)                         " +
            "); " +

            "CREATE INDEX IF NOT EXISTS idx_sources_ts ON sources (ts); "
        );

        this.createDataTables();

        this.prepareSQL({
            begin:    'BEGIN',
            commit:   'COMMIT',
            rollback: 'ROLLBACK',

            insertAuthor:
                "INSERT OR IGNORE INTO authors(aid, cert)" +
                "VALUES($aid, $cert)"
            ,
            insertDataSource:
                "INSERT OR IGNORE INTO sources(ts, ds, cert)" +
                "VALUES(CURRENT_TIMESTAMP, $ds, $cert)"
            ,
            selectAllSources:
                "SELECT ds, seq" +
                " FROM sources" +
                " WHERE seq>0"
                //todo: add limit + offset
                //" ORDER BY ts DESC" +
                //" LIMIT $limit"
            ,
            selectSourcesByPeriod:
                "SELECT ds, seq" +
                " FROM sources" +
                " WHERE ts >= $date AND seq>0"
            ,
            selectSourceInfo:
                "SELECT ds, cert, seq, size," +
                " (select dshash from data where ds=$ds order by seq desc limit 1) as dshash " +
                " FROM sources" +
                " WHERE ds = $ds" +
                " LIMIT 1"
        });

        //--- init data source --------
        this.getDataSourceInfo(this.ds, function(err, src){
            if(err) throw err;
            if(src.cert) return fn();
            src.cert = this.cert;
            this._sql.insertDataSource.run({
                $ds: this.ds,
                $cert: src.cert.toBuffer()
            }, function(err){
                if(err) throw err;
                fn();
            });
        }.bind(this));

        this.db.wait();
    },

    //--------------- HTTP -----------------
    getHttpInfo: function() {
        return {
            usage: (this.usedSize/this.getAllocatedSize()*10000|0)*0.01
        }
    },

    /**
     * Process http-request to segment data /-/A<segNum:oct>/...
     *
     * @param conn
     */
    processHttpRequest: function(conn) {
        if(conn.getRequestHeader('if-modified-since')) {
            return conn.response(304);
        }
        if(!this.isInitialized()) {
            return conn.response(503);
        }
        switch(conn.urlParts[3]) {
            case "add": // add data to stream
                return this.httpCommandAdd(conn);

            case "data": // list elements of stream
                return this.httpCommandData(conn);

            case "sources": // list data sources
                return this.httpCommandSources(conn);

            case "source": // get data of source by ds
                return this.httpCommandSource(conn);

            case "notify": // notify me about new data
                return this.httpCommandNotify(conn);

            case "listen": // listen changes uid
                return this.httpCommandListen(conn);

            default:
                return conn.response400();
        }
    },

    /**
     * Add data to stream
     *
     * 	process POST request	/-/<seg>/add
     * 	POST-params: uid, pos, author, ver, data, sign [, hash]
     */
    httpCommandAdd: function(conn) {
        if(this.usedSize >= this.getAllocatedSize()) {
            return conn.response(507);
        }
        if(conn.method !== 'POST') return conn.response400('Method Not Allowed');
        var src = this.sources[this.ds]; // current data source
        if(!src || !src.cert) return conn.response500();
        conn.setMaxPostSize(this.getMaxPostSize());
        conn.onPostData(function(post) {
            var q = this._insQueue || (this._insQueue = []);
            if(q.length > 1000) return conn.response(503);

            // todo: add anti-flood statistic by author
            var ins = this.prepareData(post, src);
            if(ins.err) return conn.response400(ins.err);
            ins.$ds = src.ds;
            ins.conn = conn;
            q.push(ins);
            if(q.length == 1) {
                var _processQueue = function() {
                    if(!q.length) return;
                    this.insertData(q[0], function(err) {
                        var ins = q.shift();
                        if(err && err.errno !== sqlite3.CONSTRAINT) {// error and is not duplicate key (hash)
                            this._insQueue = null; // clear queue
                            return ins.conn.response500(err);
                        }
                        ins.conn.response(200, this.makeURI(ins));
                        if(q.length % 5 == 0) this.notifyNodes(this.ds);
                        this.notifyListeners(ins);
                        _processQueue();

                    }.bind(this));
                }.bind(this);
                _processQueue();
            }

        }.bind(this));
    },

    httpCommandData: function(conn) {
        throw 'Abstract method'
    },

    /**
     * process GET-request:
     * 		/-/<seg>/listen/<uid>
     */
    httpCommandListen: function(conn) {
        conn.response400('Method Not Allowed');
    },

    notifyListeners: function() {
        // abstract method
    },

    /**
     * process GET-request:
     * 		/-/<seg>/sources?period=<seconds:int>
     */
    httpCommandSources: function(conn) {
        if(conn.method !== 'GET') return conn.response400('Method Not Allowed');
        if(conn.query.period !== undefined) {
            conn.responseJSONBySql(this._sql.selectSourcesByPeriod, {
                $date: _.date(-(conn.query.period|0)).toISOString()
                // $limit: Math.min(10000, query.limit|0 || 10000)
            });
        } else {
            conn.responseJSONBySql(this._sql.selectAllSources);
        }
    },

    /**
     * Get data by source
     * process GET-request:
     * 		/-/<seg>/source?ds=<ds>&seq=<seq:int>&limit=<limit:int>
     */
    httpCommandSource: function(conn) {
        if(conn.method !== 'GET') return conn.response400('Method Not Allowed');
        var ds = conn.query.ds;
        var seq = conn.query.seq|0;
        var limit = Math.min(5000, conn.query.limit|0 || 1000);
        if(!this.checkDs(ds)) return conn.response400('Bad ds');
        this.getDataSourceInfo(ds, function(err, src) {
            if(err) return conn.response500(err);
            if(!src || !src.cert) {
                delete this.sources[ds];
                return conn.response(404, 'Not found');
            }
            conn.setResponseHeader('x-base-cert', src.cert.toString());
            conn.responseJSONBySql(this._sql.selectSourceData, {
                $ds: ds,
                $seq: seq,
                $limit: limit
            });
        }.bind(this));
    },

    /**
     * Notify about new data
     * process GET-request:
     * 		/-/<seg>/notify?ds=<ds>&seq=<seq:int>
     * request-header:
     * 		x-base-cert: <ip>/<port>
     */
    httpCommandNotify: function(conn) {
        if(conn.method !== 'GET') return conn.response400('Method Not Allowed');
        var query = conn.query;
        var ds = query.ds;
        var seq = query.seq|0;
        if(!seq) return conn.response400('Bad seq');
        if(!this.checkDs(ds)) return conn.response400('Bad ds');
        var nid = this.server.parseNodeHeader(conn);
        if(!nid) return conn.response400('Bad node-header');
        var res = {};
        res[ds] = (this.sources[ds] || {}).seq|0;
        conn.responseJSON(res);

        this.syncDataSource(ds, seq, nid, function(err, success){
            if(success) this.notifyNodes(ds, nid);
        }.bind(this));
    },

    //-------- data source sequences ------------
    getDataSourceInfo: function(ds, fn) {
        var src = this.sources[ds] || (this.sources[ds] = { ds: ds, seq: 0, size: 0 });
        if(src.cert) return fn(null, src);
        this._sql.selectSourceInfo.get({ $ds: ds }, function(err, row) {
            if(err || !row) return fn(err, src);
            row.dshash = (row.dshash||"").toString("hex");
            row.cert = Certificate.parsePublic(row.cert);
            fn(err, _.ex(src, row));
        });
    },

    //----------- sync data source -----------
    notifyNodes: function(ds, notifier) {
        this.requestToNodes({
            path: 'notify?ds='+ds + '&seq=' + this.sources[ds].seq,
            excludeNode: notifier
        });
    },

    syncDataSource: function(ds, seq, nid, fn) {
        this.getDataSourceInfo(ds, function(err, src) {
            if(err) return fn(err);
            if(seq <= src.seq || seq <= src._seq) return fn(null, false);
            src._nid = nid;
            src._seq = seq;
            if(src.nid) return fn(null, false);
            src.nid = nid; // lock source
            this._syncDataSource(src, function(err, res) {
                src.nid = null; // unlock source
                fn(err, res);
            });
        }.bind(this));
    },

    _syncDataSource: function(src, fn) {
        this._sql.begin.run(this._syncDataSourceInTransaction.bind(this, src, fn));
    },

    _syncDataSourceInTransaction: function(src, fn) {
        var nid = src.nid, ds = src.ds, cnt = 0, lastRow;
        var conn = this.server.requestToNode({
            nid: nid,
            path: '/-/'+this.seg+'/source?ds='+ds+'&seq='+src.seq,
            type: 'json',
            async: true,
            onData: function(row) {
                if(!src.cert) { // unknow source. check and save pubkey
                    var srcCert64 = conn.getResponseHeader('x-base-cert');
                    if(!srcCert64) throw 'Empty x-base-cert header';
                    var cert = Certificate.parsePublic(srcCert64);
                    if(!cert || ds != cert.getID()) throw 'Incorrect x-base-cert header: ' + srcCert64;
                    src.cert = cert;
                    this._sql.insertDataSource.run({
                        $ds: ds,
                        $cert: src.cert.toBuffer()
                    });
                }
                // check sequence, data, hash, sign
                if(!row.seq) throw 'Empty seq';
                if(row.seq <= src.seq) return;
                if(row.seq != src.seq + 1) throw 'Unexpected sequence';
                var ins = this.prepareData(row, src, nid);
                if(ins.err) return conn.abort(ins.err);

                var seq = row.seq | 0;
                var hashHex = _.hex(row.hash, "base64");
                var $dsHash = _.buf(row.dshash, "base64");
                var $dsSign = _.buf(row.dssign, "base64");
                // verify data by source
                var dsHashHex = _.sha256(_.str(src.dshash || src.cert.pub + this.seg) + hashHex);
                if(dsHashHex !== $dsHash.toString("hex")) throw 'Invalid ds-hash';
                ins.$ds = ds;
                ins.$seq = seq;
                ins.$dshash = $dsHash;
                ins.$dssign = $dsSign;
                cnt++;
                this.insertData(ins, function(err) {
                    if(err) return conn.abort(err);
                    lastRow = ins;
                    conn.next();
                });
            }.bind(this),
            onFinish: function(err) {
                if(!cnt) err = 'Empty data';
                if(!err && lastRow) {
                    // check ds-sign
                    if(!src.cert.verify(lastRow.$dshash.toString("hex"), lastRow.$dssign.toString("hex"))) err = 'Invalid ds-sign';
                }
                this._sql[err? 'rollback' : 'commit'].run(function(errCommit){
                    //log('---- COMMIT ', cnt, 'records, time:',+new Date()-t0,'msec, avg:',(+new Date()-t0)/cnt|0,'msec');
                    if(err = err || errCommit) {
                        this.sources[ds] = null; // clear ds-cache
                        return fn(err);
                    }
                    if(src._nid == nid && src._seq > src.seq) // continue sync
                        return this._syncDataSource(src, fn);
                    fn(null, true); // - success sync

                }.bind(this));
            }.bind(this)
        });
    },

    insertData: function(data, callback) { // overwrite function
        var src = this.sources[data.$ds];
        if(!data.$seq) { //new data
            data.$seq = (src.seq || 0) + 1;
            data.$dshash = _.sha256((src.dshash || this.cert.pub + this.seg) + data.$hash.toString('hex'), 'utf8', 'buffer');
            data.$dssign = this.cert.sign(data.$dshash);
        }
        this._sql.insertData.run(data, function(err) {
            if(src.seq != data.$seq - 1) err = 'Sequence was corrupted';
            if(err) return callback.call(this, err);
            src.seq = data.$seq;
            src.dshash = data.$dshash.toString("hex");
            callback.call(this, null, src.seq);
        }.bind(this));
    },

    makeURI: function(data) {
        return this.seg[0] + this.ring + "/" + (data.$uid || data.$hash).toString("hex");
    },

    requestToNodes: function(op) {
        op = _.ex({
            path: null,
            type: 'json',
            countNodes: 2,
            excludeNode: null,
            onData: null,
            onFinish: null
        }, op);
        var seg = this.seg, path = '/-/'+seg+'/' + op.path;
        var server = this.server, errors, i, nodes = server.getNodesBySegment(seg, op.countNodes);
        if(op.excludeNode && (i = nodes.indexOf(op.excludeNode)) >= 0) nodes.splice(i, 1);
        var cnt = nodes.length;
        if(!cnt) return op.onFinish && op.onFinish('Not found nodes');
        nodes.forEach(function(nid) {
            // todo: add limit on count of opened requests
            server.requestToNode({
                nid: nid,
                path: path,
                type: op.type,
                onData: op.onData && function(row) {
                    op.onData(row, nid);
                },
                onFinish: function(err) {
                    if(err) (errors = errors || {})[nid] = err;
                    if(!--cnt) op.onFinish && op.onFinish(errors, nodes);
                }
            });
        });
    },

    //---------- full sync ----------
    fullSynchronization: function(start) {
        var tStart = +new Date();
        var periodSync = 614657;  // ~10 min
        var tLastSync = this.info.tLastSync;
        if(!start && tLastSync > tStart - periodSync) {
            return setTimeout(this.fullSynchronization.bind(this), tLastSync - tStart + periodSync + Math.random() * 15551);
        }
        var self = this, sources = {};
        this.requestToNodes({
            path: 'sources/' + (tLastSync? '?period=' + ((tStart - tLastSync) * 1e-3 + 86400 |0) : ''),
            onData: function(data, nid) {
                if(!data || !self.checkDs(data.ds)) throw 'Bad data';
                var ds = data.ds, seq = data.seq|0;
                if(seq <= 0) throw 'Bad seq';
                if((!self.sources[ds] || self.sources[ds].seq < seq) && (!sources[ds] || sources[ds].seq<seq)) {
                    sources[ds] = { seq: seq, nid: nid }; // todo: use file or db
                }
            },
            onFinish: function(errors) {
                var syncSources = function(){
                    var ds = _.firstKey(sources);
                    if(ds)
                        return self.syncDataSource(ds, sources[ds].seq, sources[ds].nid, function(err, success) {
                            delete sources[ds];
                            errors = errors || err;
                            syncSources();
                        });
                    // finish sync
                    if(!errors) self.saveInfo({ tLastSync: tStart });
                    setTimeout(self.fullSynchronization.bind(self), 18181);
                };
                syncSources();
            }
        });
    }
});
