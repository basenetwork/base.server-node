var fs		= require("fs");
var crypto  = require("crypto");
var zlib    = require("zlib");
var Storage	= require("./Storage");
var _		= require("../utils");
var log 	= console.log;

module.exports = _.class(Storage, {

	constructor: function() {
		Storage.constructor.apply(this, arguments);
	},

    initStorage: function(server) {
        Storage.initStorage.apply(this, arguments);
        var filename = this.dir + '/storage.dat';
        this.storagePos = fs.existsSync(filename) && fs.statSync(filename).size || 0;
        this.storage = fs.createWriteStream(filename, { flags: 'a+' });
        this.storage.on('error', function(err) {
            throw err;
        });
    },

	//-------- public methods ----------------
	getRingSize: function() {
		return 1 << this.ring * 3; // 8^ring
	},

	getAllocatedSize: function() {
		return (4 << this.ring) *_.GiB; // 2^ring * 4GiB
	},

	getMaxPostSize: function() {
		return (1 << this.ring * 2) * 2 * _.MiB; // 4^ring 2MiB
	},

	//-------- DB -----------
	createDataTables: function() {
		this.db.exec(
			// ------ data ---------
			"CREATE TABLE IF NOT EXISTS data (			" +
			"	hash        BLOB    NOT NULL,			" + // uid. hash of file. (binary32)
			"	pos         INT     NOT NULL,           " + // data position in storage
			"	len         INT     NOT NULL,           " + // data length

			"	aid         TEXT    NOT NULL,			" + // author-id  (char20)
			"	sign        BLOB    NOT NULL,           " + // sign data by pubkey   sign(author, hash)

			"	seq         INT     NOT NULL,           " + // last sequence in data source
			"	ds          TEXT    NOT NULL,			" + // data source.
			"	dshash      BLOB    NOT NULL,           " + // ds-hash := sha256(<pre-dshash>|<hash>)
			"	dssign      BLOB    NOT NULL,           " + // sign(dsPubkey, "<hash:binary><seg:str><seq:int>")
			"	PRIMARY KEY(hash, ds)                   " +
			");" +

			"CREATE UNIQUE INDEX IF NOT EXISTS idx_data_seq  ON data (ds, seq);" +

			// ------ triggers ---------
			"CREATE TRIGGER IF NOT EXISTS data_insert AFTER INSERT ON data " +
			" BEGIN " +
			"  UPDATE sources SET size = size + length(new.len), seq = new.seq, ts = current_timestamp WHERE ds = new.ds; " +
			"  UPDATE authors SET size = size + length(new.len) WHERE aid = new.aid; " +
			" END; "
		);

		this.prepareSQL({
			insertData:
				"INSERT INTO data(hash, aid, sign, pos, len, ds, seq, dshash, dssign)" +
				"VALUES($hash, $aid, $sign, $pos, $len, $ds, $seq, $dshash, $dssign)"
			,
			selectData:
				"SELECT pos, len, sign," +
                    "authors.cert as author" +
				" FROM data" +
                " JOIN authors ON authors.aid=data.aid" +
				" WHERE hash=$hash" +
				" LIMIT 1"
			,
			selectSourceData:
				"SELECT " +
					"seq," +
					"authors.cert as author," +
					"sign," +
					"hash," +
					"dshash," +
					"dssign" +
				" FROM data" +
                " JOIN authors ON authors.aid=data.aid" +
				" WHERE ds=$ds AND seq>$seq" +
				" ORDER BY seq" +
				" LIMIT $limit"
		});
	},

	//------------- data ----------------
	prepareData: function(post, src, nid) {
		var hash = _.b64ToHex(post.hash);
		var sign = _.b64ToHex(post.sign);
		var cert = this.parseCertificate(post.author);
		if(!cert) return { err: 'Incorrect author-certificate' };
		if(!this.checkUid(hash)) return { err: 'Hash is not appropriate' };
		if(this.ring == 0 && !cert.isRegistrar()) return { err: 'Method allowed only for registrar' };
		if(!nid || this.ring == 0 || Math.random() < 0.10) { // data is uploaded by client or by registrar
			if(!cert.isRegistrar() && !cert.isSignedByRootRegistrar()) return { err: 'Certificate is not signed by registrar' };
			if(!cert.verify(this.seg + hash, sign)) return { err: 'Invalid sign' };
		}
		this.saveCertificate(cert);
		var aid = cert.getID();
		if(nid) { // data is uploaded from node
			return {
				_nid:     nid,
				$aid: 	  aid,
				$hash:    new Buffer(hash,   'hex'),
				$sign:    new Buffer(sign,   'hex'),
				$len:     post.len|0
			}
		} else { // data is uploaded by client
			var file = post.file || {};
			if(hash !== file.hash) return { err: 'Invalid hash' };
			return {
				_tmpfile: file.tmpfile,
				$aid: 	  aid,
				$hash:    new Buffer(hash,   'hex'),
				$sign:    new Buffer(sign,   'hex'),
				$len:     file.size|0
			}
		}
	},

	_appendToFileStorage: function(hash, filepath, size, fn) {
		if(this.storageLocked) return fn('Storage is locked');
		this.storageLocked = true;
		var pos = this.storagePos;
		var rs = fs.createReadStream(filepath);
		rs.on('data', function(chunk){
			this.storage.write(chunk, 'binary');
			this.storagePos += chunk.length;
		}.bind(this));
		rs.on('end', function(){
			this.storageLocked = false;
			_.unlink(filepath);
			fn(null, pos);
		}.bind(this));
	},

	_insertData: function(data, pos, fn) {
		data.$pos = pos;
		Storage.insertData.call(this, data, fn);
	},

	insertData: function(data, fn) {
		this._sql.selectData.get({ $hash: data.$hash }, function(err, row) { // check existed hash
			if(err) return fn(err);
			if(row && row.len) {
				if(data._tmpfile) return fn(); // upload by user
				return this._insertData(data, row.pos, fn);
			}
			var saveData = function(tmpfile) {
				this._appendToFileStorage(data.$hash, tmpfile, data.$len, function(err, pos) {
					if(err) return fn(err);
					this._insertData(data, pos, fn);
				}.bind(this));
			}.bind(this);
			if(data._tmpfile) {
				saveData(data._tmpfile);
			} else { // http request to node
				var hash = data.$hash.toString('hex');
				this.server.requestToNode({
					nid: data._nid,
					path: '/-/'+this.seg+'/data/'+hash,
					type: 'file',
					onFinish: function(err, file) {
						if(err) return fn(err);
						if(hash !== file.hash) return fn('Invalid hash');
						data.$len = file.size;
						saveData(file.tmpfile);
					}
				});
			}
		}.bind(this));
	},

	//--------------- HTTP -----------------
	/**
	 * process GET-request:
	 * 		/-/<seg>/data/<uid:hex>.<ext:str>
	 */
	httpCommandData: function(conn) {
		if(conn.method !== 'GET') return conn.response400('Method Not Allowed');
		var q = _.str(conn.urlParts[4]).split('.');
		var uid = q[0], ext = q[1]||'';
		if(!this.checkUid(uid)) return conn.response400('Isn`t appropriate uid');
		this._sql.selectData.get({
			$hash: new Buffer(uid, 'hex')
		}, function(err, row) {
			if(err) return conn.response500('Can`t read DB');
			if(!row) return conn.response(404, 'UID not found');
            var len = row.len;
            conn.setExpire();
			conn.setResponseHeader("Accept-Ranges", "bytes");
			conn.setResponseHeader('x-base-author', row.author.toString("base64") + '; sign=' + row.sign.toString("base64"), true);
			conn.setContentType(ext);

            var code = 200, rsOp = { start:row.pos, end:row.pos + len-1 };
            if(conn._req.headers.range) {
                var range = String(conn._req.headers.range).replace(/bytes=/, "").split("-");
                var rng0 = Math.max(0, Math.min(len-1, range[0]|0));
                var rng1 = Math.max(0, Math.min(len-1, range[1]|0));
                if(rng1 <= rng0) rng1 = len - 1;
                rsOp.start = row.pos + rng0;
                rsOp.end   = row.pos + rng1;
                code = 206;
			    conn.setResponseHeader("Content-Range", "bytes " + rng0 + "-" + rng1 + "/" + len);
            } else {
                conn.setResponseHeader("Content-Length", len);
            }
            var rs = fs.createReadStream(this.dir + '/storage.dat', rsOp);
            conn.response(code, rs);
		}.bind(this));
	}

});
