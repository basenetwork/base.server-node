var Storage = require("./Storage");
var Data    = require("./Data");
var _       = require("../utils");
var log     = console.log;

module.exports = _.class(Data, {

    constructor: function() {
        Storage.constructor.apply(this, arguments);
    },

    //-------- public methods ----------------
    getRingSize: function() {
        return 1 << this.ring * 3; // 8^ring
    },

    getAllocatedSize: function() {
        return (2 << this.ring) * _.GiB; // 2^ring * 2GiB
    },

    getMaxPostSize: function() {
        return (2 << this.ring * 3) * _.KiB; // 8^ring * 2KiB
    },

    //-------- DB -----------
    createDataTables: function() {
        this.db.exec(
            // ------ data ---------
            "CREATE TABLE IF NOT EXISTS data (           " +
            "    actual      BOOLEAN NOT NULL DEFAULT 1, " + // record is actual in stream
            "    uid         BLOB    NOT NULL,           " + // stream-id  (binary32)
            "    idx         BLOB    NOT NULL,           " + // uniq.position in stream := pos + aid (binary40)
            "    aid         TEXT    NOT NULL,           " + // author-id  (char20)
            "    ver         INT     NOT NULL,           " +    // version of data := version<<32|hash

            "    hash        BLOB    NOT NULL,           " + // sha256("<uid:hex>|<pos:utf8>|<aid:hex>|<ver:int>|<data:utf8>")  (binary32)
            "    data        BLOB    NOT NULL,           " + // data
            "    sign        BLOB    NOT NULL,           " + // sign data by pubkey   sign(author, hash)

            "    seq         INT     NOT NULL,           " + // last sequence in data source
            "    ds          TEXT    NOT NULL,           " + // data source.
            "    dshash      BLOB    NOT NULL,           " + // ds-hash := sha256(<pre-dshash>|<hash>)
            "    dssign      BLOB    NOT NULL            " + // sign(dsPubkey, "<hash:binary><seg:str><seq:int>")
            ");" +

            "CREATE        INDEX IF NOT EXISTS idx_data_idx  ON data (actual, uid, idx, ver);" +
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_data_seq  ON data (ds, seq);" +
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_data_hash ON data (hash, ds);" +

            // ------ triggers ---------
            "CREATE TRIGGER IF NOT EXISTS data_insert AFTER INSERT ON data " +
            " BEGIN " +
            "  UPDATE data SET actual=0 WHERE actual AND uid=new.uid AND idx=new.idx;" +
            "  UPDATE data SET actual=1 WHERE length(data)>0 and rowid=" +
            "   (SELECT rowid FROM data WHERE actual=0 AND uid=new.uid AND idx=new.idx ORDER BY ver desc LIMIT 1); " +
            "  UPDATE sources SET size = size + length(new.data), seq = new.seq, ts = current_timestamp WHERE ds = new.ds; " +
            "  UPDATE authors SET size = size + length(new.data) WHERE aid = new.aid; " +
            " END; "

            // todo: add statistic
        );

        var streamFields;
        this.prepareSQL({
            insertData:
                "INSERT INTO data(uid, idx, ver, aid, hash, sign, data, ds, seq, dshash, dssign)" +
                "VALUES($uid, $idx, $ver, $aid, $hash, $sign, $data, $ds, $seq, $dshash, $dssign)"
            ,
            selectTop:
                "SELECT " + (streamFields =
                    "substr(idx, 1, length(idx)-20) as pos," +
                    "ver/4294967296|0 as ver," +
                    "authors.cert as author," +
                    "hash," +
                    "sign," +
                    "data"
                ) +
                " FROM data" +
                " JOIN authors ON authors.aid=data.aid" +
                " WHERE actual AND uid=$uid" +
                " ORDER BY idx DESC" +
                " LIMIT $limit"
            ,
            selectPrev:
                "SELECT " + streamFields +
                " FROM data" +
                " JOIN authors ON authors.aid=data.aid" +
                " WHERE actual AND uid=$uid AND idx<$idx" +
                " ORDER BY idx DESC" +
                " LIMIT $limit"
            ,
            selectNext:
                "SELECT " + streamFields +
                " FROM data" +
                " JOIN authors ON authors.aid=data.aid" +
                " WHERE actual AND uid=$uid AND idx>$idx" +
                " ORDER BY idx ASC" +
                " LIMIT $limit"
            ,
            selectDocument:
                "SELECT " + streamFields +
                " FROM data" +
                " JOIN authors ON authors.aid=$aid" +
                " WHERE actual AND uid=$uid AND data.aid=$aid AND idx=$idx" +
                " LIMIT 1"
            ,
            selectOldVersions:
                "SELECT " + streamFields +
                " FROM data" +
                " JOIN authors ON authors.aid=$aid" +
                " WHERE actual=0 AND uid=$uid AND data.idx=$idx AND data.ver<$ver" +
                " ORDER BY ver DESC" +
                " LIMIT $limit"
            ,
            selectSourceData:
                "SELECT " +
                    "seq," +
                    "uid," +
                    "dshash," +
                    "dssign," +
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
        var author = _.str(post.author);    // certificate-base64
        var uid    = _.b64ToHex(post.uid);  // uid hex
        var pos    = _.b64ToBin(post.pos);  // unique name in stream or id or position into channel str(20)
        var data   = _.b64ToBin(post.data); // bin
        var sign   = _.b64ToHex(post.sign); // hex
        var hash   = _.b64ToHex(post.hash); // hex
        var ver    = post.ver|0;            // int
        if(ver<0 || ver>1e6)    return { err: 'Version is too large' };
        if(pos.length > 20)     return { err: 'Position is too long' };
        if(!this.checkUid(uid)) return { err: 'Uid is not appropriate' };
        var _hash = _.sha256([uid, pos, author, ver, data].join('|'));
        if(_hash !== hash)      return { err: 'Invalid hash' };
        var cert = this.parseCertificate(author);
        if(!cert) return { err: 'Incorrect author' };
        if(this.ring == 0 && !cert.isRegistrar()) return { err: 'Method allowed only for registrar' };
        if(!nid || this.ring == 0 || Math.random() < 0.10) {
            //if(!cert.isRegistrar() && !cert.isSignedByRootRegistrar()) return { err: 'Certificate is not signed by registrar' };
            if(!cert.verify(this.seg + hash, sign)) return { err: 'Invalid sign' };
        }
        this.saveCertificate(cert);
        var aid = cert.getID();
        return {
            $uid:   new Buffer(uid, 'hex'),
            $aid:   aid,
            $sign:  new Buffer(sign, 'hex'),
            $hash:  new Buffer(hash, 'hex'),
            $ver:   ver * 0x100000000 + parseInt(hash.substr(0, 8), 16),  // version + pseudo random number
            $idx:   new Buffer(pos + aid, "binary"), // bin(20+20)
            $pos:   new Buffer(pos, "binary"),
            $data:  new Buffer(data, "binary")
        }
    },

    makeURI: function(data) {
        var idx = data.$idx.toString("hex"); // pos + aid(20)
        return Storage.makeURI.call(this, data) + "@" + data.aid + ":" + idx.substr(idx.length - 2 * 20);
    },

    //--------------- HTTP -----------------
    /**
     * process GET-request:
     *         /-/<seg>/data/<uid:hex>?cmd=<cmd>&aid=<aid:char20>&pos=<pos:hex>&ver=<ver:int>&limit=<limit:int>
     *
     * cmd :=
     *      "top" - Get data by uid
     *      "prv" - Get previous data in stream by uid, author and last position
     *      "nxt" - Get next data in stream by uid, author and last position
     *      "doc" - Get single actual document in stream by uid, author and position
     *      "old" - Get old versions of document
     *
     */
    httpCommandData: function(conn) {
        if(conn.method !== 'GET') return conn.response400('Method Not Allowed');
        var uid = conn.urlParts[4], query = conn.query, cmd = _.str(query.cmd);
        if(!this.checkUid(uid)) return conn.response400('Isn`t appropriate uid');
        var sqlCommands = {
            top: this._sql.selectTop,
            prv: this._sql.selectPrev,
            nxt: this._sql.selectNext,
            doc: this._sql.selectDocument,
            old: this._sql.selectOldVersions
        };
        var params = {
            $uid: _.buf(uid, "hex"),
            $aid: _.str(query.aid),
            $idx: _.buf(_.buf(query.pos, "hex") + _.str(query.aid)),
            $ver:   Math.min(query.ver|0 || 1e6, 1e6) * 0x100000000,
            $limit: Math.max(1, Math.min(1000, query.limit|0 || 100))
        };
        //if(!params.$idx) return conn.response400("Bad offset");
        conn.responseJSONBySql(sqlCommands[cmd] || sqlCommands.top, params);
    }

});
