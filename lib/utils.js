var fs     = require('fs');
var os     = require("os");
var crypto = require('crypto');

module.exports = {

    KiB: 1<<10,
    MiB: 1<<20,
    GiB: 1<<30,

    class: function() {
        var cls = arguments[arguments.length - 1].constructor || function(){};
        // for(var iArg = 0; iArg < arguments.length - 1; iArg++) util.inherits(cls, arguments[iArg]);
        for(var iArg = 0; iArg < arguments.length; iArg++)
            for(var i in arguments[iArg])
                cls[i] = cls.prototype[i] = arguments[iArg][i];
        return cls;
    },

    noException: function(fn, exceptionValue) {
        return function() {
            try {
                return fn.apply(this, arguments);
            } catch(e) {
                return exceptionValue;
            }
        };
    },

    parseJSON: function(str) {
        try {
            return JSON.parse(str.toString());
        } catch(e) {
            return null;
        }
    },

    str: function(data) {
        try {
            return data.toString("binary");
        } catch(e) {
            return "";
        }
    },

    hex: function(str, encoding) {
        try {
            return new Buffer(str, encoding || "binary").toString("hex");
        } catch(e) {
            return "";
        }
    },

    buf: function(str, encoding) {
        try {
            return new Buffer(str, encoding || "binary");
        } catch(e) {
            return null;
        }
    },

    b64ToBin: function(base64, enc) {
        try {
            return new Buffer(base64, "base64").toString(enc||"binary");
        } catch(e) {
            return "";
        }
    },

    b64ToHex: function(base64) {
        return this.hex(base64, "base64");
    },

    date: function(intervalSec) {
        var d = new Date();
        d.setSeconds(d.getSeconds() + (intervalSec|0));
        return d;
    },

    ex: function(a, b) {
        a = a || {};
        if(b) for(var i in b) a[i]=b[i];
        return a;
    },

    obj: function(key, val) {
        var obj = {};
        obj[key] = val;
        return obj;
    },

    size: function(obj) {
        var len = 0;
        if(obj) for(var i in obj) len++;
        return len;
    },

    firstKey: function(obj) {
        if(obj) for(var i in obj) return i;
    },

    randomKey: function(obj) {
        var keys = Object.keys(obj);
        return keys[(Math.random() * keys.length)|0];
    },

    shuffle: function(arr, count) {
        if(arr instanceof Array) {
            var a = arr.slice(), res = [];
            if(count === undefined) count = a.length;
            while(count-- && a.length) res.push(a.splice(Math.random() * a.length|0, 1)[0]);
            return res;
        }
    },

    sha256: function(data, encoding, outEncoding) {
        return crypto.createHash("sha256").update(data, encoding || "binary").digest(outEncoding || "hex");
    },

    escapeBase64: function(base64) {
        return base64.replace(/\+/g, '-').replace(/\//g, '_');
    },

    formatSize: function(num, digits) {
        if(digits === undefined) digits = 3;
        if(num < 1024) return num + ' bytes';             num /= 1024;
        if(num < 1024) return num.toFixed(digits)+' KiB'; num /= 1024;
        if(num < 1024) return num.toFixed(digits)+' MiB'; num /= 1024;
        if(num < 1024) return num.toFixed(digits)+' GiB'; num /= 1024;
        if(num < 1024) return num.toFixed(digits)+' TiB'; num /= 1024;
        if(num < 1024) return num.toFixed(digits)+' PiB'; num /= 1024;
        return num.toFixed(digits)+' EiB';
    },

    compareIP: function(ip1, ip2) {
        if(typeof ip1 !== "string") return false;
        if(typeof ip2 !== "string") return false;
        if((ip1.indexOf(':') < 0) !== (ip2.indexOf(':') < 0)) {
            return ip1.indexOf(':') >= 0 ? 1 : -1;
        }
        ip1 = ip1.split(/[^0-9a-f]/);
        ip2 = ip2.split(/[^0-9a-f]/);
        for(var i in ip1) if(ip1[i]!==ip2[i]) return (parseInt(ip1[i]||0,16) > parseInt(ip2[i]||0,16))*2 - 1;
        return 0;
    },

    mkdir: function(path, mode) {
        var fs = require("fs");
        if(!fs.existsSync(path)) {
            fs.mkdirSync(path, mode || 0755);
            if(!fs.existsSync(path)) {
                throw "Can`t create directory `"+path+"`";
            }
        }
        return path;
    },

    createTempStream: function(callback) {
        var strm, path = os.tmpDir() + '/basenetwork-' + crypto.createHash("md5").update(Math.random().toString() + +new Date()).digest("hex");
        try {
            strm = fs.createWriteStream(path);
        } catch(e) {
            callback && callback(e);
            return null;
        }
        strm._tmpfile = path;
        strm.on('finish', function() {
            callback && callback();
            // delay remove file
            setTimeout(function(){
                fs.unlink(path, function(){});
            }, 10e3);
        });
        callback && strm.on('error', callback);
        return strm;
    },

    unlink: function(filepath) {
        try {
            fs.unlink(filepath);
        } catch(e) {}
    },

    cache: function(maxLength) {
        return {
            data: {},
            length: 0,
            clear: function() {
                this.length = 0;
                this.data = {};
            },
            anyKey: function() {
                var keys = Object.keys(this.data);
                return keys[Math.random()*keys.length |0];
            },
            get: function(key) {
                return this.data[key];
            },
            set: function(key, val) {
                if(this.data[key] === undefined) this.length++;
                if(val === null) return this.unset(key);
                if(val === undefined) val = this.data[key] || {};
                this.data[key] = val;
                if(this.length > maxLength) {
                    for(var i in this.data) return this.unset(i);
                }
            },
            unset: function(key) {
                if(this.data[key] !== undefined) {
                    this.length--;
                    delete this.data[key];
                }
            }
        };
    }
};
