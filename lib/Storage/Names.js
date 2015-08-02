var crypto  = require("crypto");
var Storage	= require("./Storage");
var _		= require("../utils");
var Certificate	= require("../Certificate");
var log 	= console.log;

module.exports = _.class(Storage, {

	constructor: function() {
		Storage.constructor.apply(this, arguments);
	},

	//-------- public methods ----------------
	getRingSize: function() {
		return 1 << this.ring * 3; // 8^ring
	},

	getAllocatedSize: function() {
		return (8 << this.ring) * _.GiB; // 2^ring * 8GiB
	},

	getMaxPostSize: function() {
		return (4 << this.ring * 2) * _.KiB; // 4^ring * 4KiB
	},

	//-------- DB -----------
	createDataTables: function() {
		this.db.exec(
			// ------ data ---------
			"CREATE TABLE IF NOT EXISTS data (			" +
			"	actual      BOOLEAN NOT NULL DEFAULT 1, " + // record is actual in stream
			"	uid         BLOB    NOT NULL,			" + // uniq document id  (binary32)
			"	ver         INT     NOT NULL,			" +	// version of data   (version<<32|hash)
			"	aid         TEXT    NOT NULL,			" + // author-id  (char20)
			"	hash        BLOB    NOT NULL,           " + // sha256("<uid:hex>|<author:hex>|<ver:int>|<data:utf8>")  (binary32)

			"	sign        BLOB    NOT NULL,           " + // sign data by pubkey   sign(author, hash)
			"	data        BLOB    NOT NULL,           " + // data

			"	seq         INT     NOT NULL,           " + // last sequence in data source
			"	ds          TEXT    NOT NULL,			" + // data source.
			"	dshash      BLOB    NOT NULL,           " + // ds-hash := sha256(<pre-dshash>|<hash>)
			"	dssign      BLOB    NOT NULL            " + // sign(dsPubkey, "<hash:binary><seg:str><seq:int>")
			");" +

			"CREATE        INDEX IF NOT EXISTS idx_data_uniq ON data (uid, aid, ver) WHERE(actual);" +
			"CREATE UNIQUE INDEX IF NOT EXISTS idx_data_seq  ON data (ds, seq);" +
			"CREATE UNIQUE INDEX IF NOT EXISTS idx_data_hash ON data (hash, ds);" +

			// ------ triggers ---------
			"CREATE TRIGGER IF NOT EXISTS data_insert AFTER INSERT ON data " +
			" BEGIN " +
            "  UPDATE data SET actual=0 WHERE actual AND uid=new.uid AND aid=new.aid;" +
            "  UPDATE data SET actual=1 WHERE length(data)>0 and rowid=" +
            "   (SELECT rowid FROM data WHERE actual=0 AND uid=new.uid AND aid=new.aid ORDER BY ver desc LIMIT 1); " +
            "  UPDATE sources SET size = size + length(new.data), seq = new.seq, ts = current_timestamp WHERE ds = new.ds; " +
			"  UPDATE authors SET size = size + length(new.data) WHERE aid = new.aid; " +
			" END; "
		);

		var streamFields;
		this.prepareSQL({
			insertData:
				"INSERT INTO data(uid, aid, ver, hash, sign, data, ds, seq, dshash, dssign)" +
				"VALUES($uid, $aid, $ver, $hash, $sign, $data, $ds, $seq, $dshash, $dssign)"
			,
			selectData:
				"SELECT " + (streamFields =
					"ver/4294967296|0 as ver," +
					"authors.cert as author," +
					"hash, sign, data"
				) +
				" FROM data" +
                " JOIN authors ON authors.aid=$aid" +
				" WHERE actual AND uid=$uid AND data.aid=$aid" +
				" LIMIT 1"
			,
			selectSourceData:
				"SELECT " +
					" seq, uid, dshash, dssign," +
					streamFields +
				" FROM data" +
                " JOIN authors ON authors.aid=data.aid" +
				" WHERE ds=$ds AND seq>$seq" +
				" ORDER BY seq" +
				" LIMIT $limit"
		});
	},

	//------------- data ----------------
	prepareData: function(post, src, nid) {
		// todo: check version of protocol
		var author	= _.str(post.author);		// certificate-base64
		var uid		= _.b64ToHex(post.uid);		// stream id hex(96)
		var sign	= _.b64ToHex(post.sign);	// hex
		var hash	= _.b64ToHex(post.hash);	// hex
		var data	= _.b64ToStr(post.data);	// utf8
		var ver		= post.ver|0;			// int
		if(!data.length) 		return { err: 'Empty data' };
		if(ver<0 || ver>1e6) 	return { err: 'Version is too large' };
		if(!this.checkUid(uid)) return { err: 'UID is not appropriate' };
		var _hash = _.sha256([uid, author, ver, data].join('|'));
		if(hash !== _hash) 	return { err: 'Invalid hash' };
		var cert = this.parseCertificate(author);
		if(!cert) return { err: 'Incorrect author' };
		if(!cert.isRegistrar()) return { err: 'Method allowed only for registrar' };
		//if(!src || !src.cert || !src.cert.isRegistrar()) return { err: 'It is not source of registrar' };
		if(!cert.verify(this.seg + hash, sign)) return { err: 'Invalid sign' };
		this.saveCertificate(cert);
		return {
			$uid: 	new Buffer(uid, 'hex'),
			$hash:	new Buffer(hash, 'hex'),
			$aid:	cert.getID(),
			$ver:  	ver * 0x100000000 + parseInt(hash.substr(0, 8), 16),  // version + pseudo random number
			$sign:	new Buffer(sign, 'hex'),
			$data:	new Buffer(data, 'utf8')
		}
	},

	//--------------- HTTP -----------------
	/**
	 * process GET-request:
	 * 		/-/<seg>/data/<uid:hex>?aid=<aid>
	 * 		/-/<seg>/data/<domain:str>
	 */
	httpCommandData: function(conn) {
        // todo: add expire 1day
		if(conn.method !== 'GET') return conn.response400('Method Not Allowed');
        var m, aid, uid = _.str(conn.urlParts[4]);
        if(this.checkUid(uid)) {
            aid = conn.query.aid || Certificate.getRegistrarCertificateByZone('base').getID();
            if(!this.checkCertID(aid)) return conn.response400('Author-id param is not appropriate');

        } else if(m = uid.match(/^([a-z0-9_\-\.]+\.)?([a-z0-9]{1,5})$/)) {
            var cert = Certificate.getRegistrarCertificateByZone(m[2]);
            if(!cert) return conn.response400('Zone .'+m[2]+' is not appropriate');
            aid = cert.getID();
            uid = _.sha256(m[0], 'binary', 'hex');
        } else {
            return conn.response400('Path is not appropriate');
        }
		conn.responseJSONBySql(this._sql.selectData, {
			$uid: new Buffer(uid, 'hex'),
			$aid: aid
		});
	}

});
