var fs  		= require("fs");
var KJUR		= require("jsrsasign");
var BigInteger  = require("jsrsasign").BigInteger;
var _ 			= require("./utils");
var log 		= console.log;

var CERTIFICATE_VER	= 0;
var ecdsa = new KJUR.crypto.ECDSA({curve: "secp256k1"});
var ecdsaKeyLen = ecdsa.ecparams.keylen / 4;
var _regCerts = {};
var _certs = _.cache(50000);

// ---- redefine ecdsa format functions -------
KJUR.crypto.ECDSA.biRSSigToASN1Sig = function(x, y) {
    return ("000000000000000" + x.toString(16)).slice(-ecdsaKeyLen)
         + ("000000000000000" + y.toString(16)).slice(-ecdsaKeyLen);
};
KJUR.crypto.ECDSA.parseSigHex = function(signHex) {
    return {
        r: new BigInteger(signHex.substr(0, ecdsaKeyLen), 16),
        s: new BigInteger(signHex.substr(ecdsaKeyLen), 16)
    }
};
//ECPointFp.decodeFromHex = function(g, c) {
//    var a = new BigInteger(c.substr(0, ecdsaKeyLen), 16);
//    var h = new BigInteger(c.substr(ecdsaKeyLen), 16);
//    return new ECPointFp(g, g.fromBigInteger(a), g.fromBigInteger(h))
//};


/**
 * 	private certificate:
 * 		{"ver":<version:num>,"prv":"<prvkey:hex>","pub":"<pubkey:hex>"}
 * 		    
 * 	public certificate: 
 * 		<version:1byte><pubkey:64bytes>:base64
 * 		    
 * 	public certificate signed by registrar:
 * 		<version:1byte><pubkey:64bytes><registrar_sign:72bytes>:base64
 */
var Certificate;
module.exports = Certificate = _.class({

	ver: null,
	pub: null,
	prv: null,
	rsign: "",

	constructor: function() {
	},

	//------- static methods -----------------
	loadPrivate: function(filename) {
		fs.existsSync(filename) || this.generate(filename);
		var keys = JSON.parse(fs.readFileSync(filename)); // -> { ver, prv }
		if(keys.ver != CERTIFICATE_VER) throw "Unknown format of certificate-file " + filename;
        return this._getByPrvHex(keys.prv);
	},

    _getByPrvHex: function(hexPrvKey) {
		var cert = new Certificate();
		cert.ver = CERTIFICATE_VER;
		cert.prv = hexPrvKey;
        cert.pub = this._getPublicKeyByPrivate(cert.prv);
		return cert;
	},

    _getPublicKeyByPrivate: function(prvHex) {
        var p = new BigInteger(prvHex, 16);
        var m = ecdsa.ecparams.G.multiply(p);
        return ("000000000000000" + m.getX().toBigInteger().toString(16)).slice(-ecdsaKeyLen)
             + ("000000000000000" + m.getY().toBigInteger().toString(16)).slice(-ecdsaKeyLen);
    },

    _generatePrivateKey: function() {
        var k = ecdsa.ecparams.n;
        return ("000000000000000" + ecdsa.getBigRandom(k).toString(16)).slice(-ecdsaKeyLen);
    },

	generate: function(filename) {
		// todo: check collisions of cert ID
		fs.writeFileSync(filename, JSON.stringify({
			ver: CERTIFICATE_VER,
			prv: this._generatePrivateKey()
		}), { mode: 0600 });
		if(!fs.existsSync(filename)) throw 'Can`t save certificate file ' + filename;
	},

	parsePublic: function(cert64) {
		if(cert64 instanceof Certificate) return cert64;
		if(_certs.get(cert64)) return _certs.get(cert64);
		var buf, cert = new Certificate();
		try {
			buf = new Buffer(cert64, 'base64');
		} catch(e) {
			return null;
		}
		if(buf.length < 65) return null; // Bad certificate
		var hex = buf.toString("hex"), ver = parseInt(hex.substr(0, 2), 16)|0;
		if(ver !== CERTIFICATE_VER) return null; //throw "Bad certificate";
		cert.ver = ver;
		cert.pub = hex.substr(2, 128) || "";
		cert.rsign = hex.substr(130, 128) || ""; // sign of registrar
		_certs.set(cert64, cert);
		return cert;
	},

	//--------- export -----------
	toString: function(encoding) {
		return this.toBuffer().toString(encoding || "base64");
	},

	toBuffer: function() {
		return this._buf || (this._buf = new Buffer(("0"+(this.ver|0).toString(16)).slice(-2) + this.pub + this.rsign, "hex"));
	},

	getID: function() {
		return this._id || (this._id = _.escapeBase64(
            _.sha256(this.toBuffer(), "buffer", "base64").substr(0, 20)
		));
	},

	//------- sign -----------
	sign: function(data) {
		if(!this.prv) return false;
		if(data instanceof Buffer) data = data.toString("hex");
		var hash = /^[0-9a-f]{64}$/.test(data)? data : _.sha256(_.str(data), "binary", "hex");
		try {
			var signHex = ecdsa.signHex(hash, this.prv);
			return new Buffer(signHex, "hex");
		} catch(e) {
			return false;
		}
	},

	//-------- verify --------------
	verify: function(data, sign) {
		if(!this.pub) return null;
		var hash = /^[0-9a-f]{64}$/.test(data)? data : _.sha256(_.str(data), "binary", "hex");
		try {
			return ecdsa.verifyHex(hash, _.str(sign), "04" + this.pub);
		} catch(e) {
			return false;
		}
	},

	//------- registrars ---------
	_initRegistrars: function() {
		var all = JSON.parse(fs.readFileSync(__dirname + "/../data/registrars.json").toString());
		all.forEach(function(reg) {
			var cert = Certificate.parsePublic(reg.cert);
			_regCerts[cert.pub] = _regCerts[reg.zoneDNS] = _regCerts[reg.zone] = cert;
		});
	},

	isRegistrar: function() {
		return !!_regCerts[this.pub];
	},

	getRegistrarCertificateByZone: function(zone) {
		return _regCerts[zone];
	},
	
	isSignedByRootRegistrar: function() {
//		if(this.isRegistrar()) return true;
		if(!this.rsign) return false;
		return this._valid = this._valid || _regCerts.base.verify(this.pub, this.rsign);
	}
});

Certificate._initRegistrars();

