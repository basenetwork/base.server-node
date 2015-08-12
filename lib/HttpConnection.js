var fs          = require('fs');
var zlib        = require('zlib');
var Stream      = require('stream').Stream;
var URL 		= require("url");
var http    	= require('http');
var crypto  	= require("crypto");
var querystring	= require("querystring");
var _		    = require("./utils");
var log         = console.log;

Buffer.prototype.toJSON = function() {
    return this.toString("base64");
};

var HTTP_CODES = {
    200: '',

    304: 'Not Modified',

    400: 'Bad Request',
    404: 'Not found',
    408: 'Request Timeout',
    413: 'Request Entity Too Large',
    415: 'Unsupported Media Type',

    500: 'Internal Server Error',
    503: 'Service Unavailable',
    507: 'Insufficient Storage'
};

var MIME_TYPES = JSON.parse(fs.readFileSync(__dirname + '/../data/mime_types.json').toString());
var _decodeURIComponent = _.noException(decodeURIComponent, '');
var connectionID = 0;

module.exports = _.class({
    
    id: null,
    _req: null,
    _res: null,
    _url: null,

    method: null,
    urlPath: null,
    urlParts: null,
    query: null,

    constructor: function(request, response) {
        this.id = ++connectionID;
        this._req = request;
        this._res = response;
        this._url = URL.parse(request.url);
        this.method = request.method;
        this.urlPath = _.str(this._url.pathname);
        this.urlParts = this.urlPath.split('/').map(_decodeURIComponent);
        this.query = querystring.parse(this._url.query) || {};

        response.statusCode = 200;
        response.setHeader('Content-Type', 'text/plain; charset=UTF-8');
    },

    getRemoteAddress: function() {
        return (this._req.connection||{}).remoteAddress
            || (this._req.socket||{}).remoteAddress;
    },

    getRequestHeader: function(name) {
        return this._req.headers[name];
    },

    setResponseHeader: function(name, value, publicAccess) {
        if(publicAccess) {
            this._res.setHeader('Access-Control-Expose-Headers', name);
        }
        this._res.setHeader(name, value);
        return this;
    },

    setContentType: function(ext) {
        return this.setResponseHeader('Content-Type', MIME_TYPES[ext] || 'application/octet-stream');
    },

    setExpire: function() {
        return this
            .setResponseHeader('Cache-Control', 'max-age=315360000, public')
            .setResponseHeader('Last-Modified', new Date().toGMTString())
            .setResponseHeader('Expires', _.date(86400 * 3650).toGMTString());
    },

    response500: function(err) {
        return this.response(500, '500 - Internal Server Error.\n' + (err || ''));
    },

    response400: function(err) {
        return this.response(400, '400 - Bad request.\n' + (err || ''));
    },

    response: function(statusCode, body) {
        this._res.statusCode = statusCode = statusCode || 200;
        if(statusCode == 200) {
            if(this.method === 'GET' && this.query.cache) { // cache param
                this.setExpire();
            }
        } else {
            this._req._readableState.ended || this._req.resume();
        }
        if(body instanceof Stream) {
            body.pipe(this.getZipResponseStream());
        } else {
            body = body===undefined? HTTP_CODES[statusCode] : body || '';
            this._res.end(body);
        }
        return this;
    },

    getZipResponseStream: function() {
        var zip, acceptEncoding = this._req.headers['accept-encoding'] || '';
        if (/\bgzip\b/.test(acceptEncoding)) {
            this._res.setHeader('Content-Encoding', 'gzip');
            (zip = new zlib.Gzip()).pipe(this._res);
        } else if (/\bdeflate\b/.test(acceptEncoding)) {
            this._res.setHeader('Content-Encoding', 'deflate');
            (zip = new zlib.Deflate()).pipe(this._res);
        } else {
            zip = this._res;
        }
        return zip;
    },

    //------------ POST data -------------------
    _maxPostSize: 10e6,
    _maxPostFields: 100,

    setMaxPostSize: function(size) {
        this._maxPostSize = size;
        return this;
    },

    onPostData: function(callback) {
        var conn = this, req = this._req, contentType = req.headers['content-type'];
        if(!contentType) return this.response(415, 'Missing content-type header');
        var maxPostSize = this._maxPostSize, maxFields = this._maxPostFields, size = 0, buf = '', err;

        if(/^application\/x-www-form-urlencoded\b/i.test(contentType)) {
            req.setEncoding('utf8');
            req.on('data', function(chunk) {
                if(err) return;
                if((size += chunk.length) > maxPostSize) {
                    return conn.response(err = 413);
                }
                buf += chunk.toString("binary");
            });
            req.on('error', function(){
                if(!err) conn.response(err = 408);
            });
            req.on('end', function() {
                if(!err) callback.call(conn, querystring.parse(buf));
            });

        } else if(/^multipart\/(?:form-data|related)(?:;|$)/i.test(contentType)) {
            // todo: set timeout
            var params={}, param={}, boundary, countFields = 0, cntFiles = 0, fFinished;
            function flushParam() {
                if(param.strm) {
                    param.strm.end();
                    param.hash = param.hash.digest('hex');
                    delete param.strm;
                }
                if(param.name)
                    params[param.name] = param.hash? param : param.value||'';
                param = {};
            }
            function error(e) {
                flushParam(true);
                if(!err) conn.response(err = e);
            }
            function finish() {
                if(!err && fFinished && !cntFiles) callback.call(conn,  params);
            }
            req.on('data', function(chunk) {
                if(err) return;
                if((size += chunk.length) > maxPostSize) return error(413);
                buf += chunk.toString("binary");
                if(!boundary) {
                    boundary = (contentType.match(/;\s*boundary=([\-a-z0-9]+)/i) || buf.match(/^--([\-a-z0-9]+)\r\n/i) || {})[1];
                    if(!boundary) return error(415);
                    boundary = '--' + boundary;
                }
                for(var pos = 0, n = buf.length; pos<n; ) {
                    var i = buf.indexOf(boundary, pos);
                    if(param._hdr) { // read content
                        var val = i==-1? buf.substr(pos) : buf.substring(pos, i-2);
                        if(param.strm) {
                            param.size = (param.size||0) + val.length;
                            param.strm.write(val, "binary");
                            param.hash.update(val);
                        } else {
                            param.value = (param.value||'') + val;
                        }
                        if(i<0) { pos = n; break; }
                    }
                    if(i>=0) { // found boundary
                        pos = i + boundary.length + 2;
                        flushParam();
                        if(++countFields > maxFields) error(413);
                    }
                    while(!param._hdr && (i=buf.indexOf("\r\n", pos))>=0) { // read headers
                        var header = buf.substring(pos, i), m;
                        pos = i + 2;
                        if(!header) {
                            param._hdr = true; // done
                        } else if(m = header.match(/Content-Disposition:.*;\s*name="([a-z0-9_]+)".*?(;\s*filename="(.*?)")?/i)) {
                            param.name = m[1];
                            if(m[3]) {
                                cntFiles++;
                                param.filename = m[3];
                                param.hash = crypto.createHash('sha256');
                                param.strm = _.createTempStream(function(err){
                                    if(err) return error(500);
                                    finish(--cntFiles);
                                });
                                param.tmpfile = param.strm._tmpfile;
                            }
                        }
                        // todo: ?? process content-type.
                    }
                    if(!param._hdr) break;
                }
                buf = buf.substr(pos);
                if(buf.length > 10 * _.KiB) error(413);
            });
            req.on('error', function(){
                error(408);
            });
            req.on('end', function(){
                finish(fFinished = true);
            });

        } else {
            this.response(415, 'Unsupported content-type');
        }
        return this;
    },

    //----------- response JSON data --------------
    responseJSON: function(obj) {
        return this.response(200, JSON.stringify(obj));
    },

    responseJSONBySql: function(sqlStatement, params, fn) {
        var s = "", zip = this.getZipResponseStream();
        sqlStatement.each(sqlStatement._filterParams(params), function(err, row) {
            if(err || !row) return;
            if(fn && fn(row) === false) return;
            zip.write(s + JSON.stringify(row), "binary");
            s = "\n";
        }, function(err) {
            if(err) return this.response500(err);
            zip.end();
        }.bind(this));
    },

    //------------- request to node --------------
    requestJSON: function(req, fn) {
        if(req.zip) (req.headers || (req.headers = {}))['Accept-Encoding'] = 'gzip';
        return this.request(req, function(err, data, headers) {
            if(err) return fn(err, null, headers);
            try {
                data = JSON.parse(data);
            } catch(e) {
                return fn(e, null, headers);
            }
            fn(null, data, headers);
        });
    },

    request: function(req, fn) {
        return http.get(req, function(response) {
            if(response.statusCode != 200) {
                response.resume();
                return fn('Status code ' + response.statusCode, null, response.headers);
            }
            var res = response, content = '', limit = req.limit;
            if(response.headers['content-encoding'] === 'gzip') {
                res = res.pipe(zlib.createGunzip());
            }
            res.on("data", function(chunk) {
                content += chunk;
                if(limit && content.length > limit) {
                    response.resume();
                    content = null;
                    fn('Limit is exceeded');
                }
            }).on('end', function() {
                if(content === null) return;
                fn(null, content, response.headers);
            });
        }).on('error', function(err) {
            fn(err);
        });
    }

});
