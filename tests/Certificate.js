#!/usr/bin/env node

var Certificate = require("../lib/Certificate.js");
var tests  = require("./tests.js");
var equal  = tests.equal;

function getMyLocalCertificate() {
    return Certificate._getByPrvHex("0982c0046d9890cb44825871fbbf41dc63af0724e8c48111db92e9de6cdd271c");
}

tests.start({
    TestCertificate_generatePrivateKey: function() {
        var prvKey = Certificate._generatePrivateKey();

        equal(prvKey.length, 64);
        equal(/^[0-9a-f]{64}$/.test(prvKey), true);
    },

    TestCertificate_generatePublicKeyByPrivate: function() {
        var prvKey = "0982c0046d9890cb44825871fbbf41dc63af0724e8c48111db92e9de6cdd271c";

        var pubKey = Certificate._getPublicKeyByPrivate(prvKey);

        equal(pubKey, "1540ef392bcf34b361852dfec3a9e2b2f7c1c065f168398211da35e7eed39e8732db154f9e10f106a962ebe0f56d01b96d0ffcf6ff8d970467fc457e828450be");
    },

    TestCertificate_parsePublicCertificate: function() {
        var str64 = "ABVA7zkrzzSzYYUt/sOp4rL3wcBl8Wg5ghHaNefu056HMtsVT54Q8QapYuvg9W0BuW0P/Pb/jZcEZ/xFfoKEUL4=";

        var cert1 = Certificate.parsePublic(str64);
        var cert2 = Certificate.parsePublic(cert1);

        equal(cert2 === cert1, true);
        equal(cert1, {
            ver: 0,
            pub: "1540ef392bcf34b361852dfec3a9e2b2f7c1c065f168398211da35e7eed39e8732db154f9e10f106a962ebe0f56d01b96d0ffcf6ff8d970467fc457e828450be",
            rsign: ""
        });
    },

    TestCertificate_parsePublicRegisteredCertificate: function() {
        var cert64 = "AMPs7bmeIn7QUZi/Xt+Max/n7YsNWCzpcHR2cbieecB6aU3lrtitX39D2ltPaEhm+dyEGtYNUBeBkU2GrhV1kUGWAABSdRXoFs2ttEckWfs82HVAddJF0cNEupxNukQRqara/918TjAjC61zJREGQoLmNrmrpPqDbZyoNY+C0lIp";

        var cert = Certificate.parsePublic(cert64);

        equal(cert, {
            ver: 0,
            pub: "c3ecedb99e227ed05198bf5edf8c6b1fe7ed8b0d582ce970747671b89e79c07a694de5aed8ad5f7f43da5b4f684866f9dc841ad60d501781914d86ae15759141",
            rsign: "960000527515e816cdadb4472459fb3cd8754075d245d1c344ba9c4dba4411a9aadaffdd7c4e30230bad732511064282e636b9aba4fa836d9ca8358f82d25229"
        });
    },

    TestCertificate_toString: function() {
        var cert = Certificate.parsePublic("AMPs7bmeIn7QUZi/Xt+Max/n7YsNWCzpcHR2cbieecB6aU3lrtitX39D2ltPaEhm+dyEGtYNUBeBkU2GrhV1kUGWAABSdRXoFs2ttEckWfs82HVAddJF0cNEupxNukQRqara/918TjAjC61zJREGQoLmNrmrpPqDbZyoNY+C0lIp");

        var str64 = cert.toString();

        equal(str64, "AMPs7bmeIn7QUZi/Xt+Max/n7YsNWCzpcHR2cbieecB6aU3lrtitX39D2ltPaEhm+dyEGtYNUBeBkU2GrhV1kUGWAABSdRXoFs2ttEckWfs82HVAddJF0cNEupxNukQRqara/918TjAjC61zJREGQoLmNrmrpPqDbZyoNY+C0lIp");
    },

    TestCertificate_getID: function() {
        var cert = Certificate.parsePublic("ABVA7zkrzzSzYYUt/sOp4rL3wcBl8Wg5ghHaNefu056HMtsVT54Q8QapYuvg9W0BuW0P/Pb/jZcEZ/xFfoKEUL4=");

        var authorID = cert.getID(); // aid

        equal(authorID, "VzEyXI2RSwQ1VVsJfMxX");
        equal(authorID.length, 20);
    },

    TestCertificate_sign: function() {
        var cert = getMyLocalCertificate();
        var data = "ABC 0123 ёпрст";

        var sign1 = cert.sign(data).toString("hex");
        var sign2 = cert.sign(data).toString("hex");

        equal(/^[0-9a-f]{128}/.test(sign1), true);
        equal(/^[0-9a-f]{128}/.test(sign2), true);
        equal(sign1 != sign2, true);
    },

    TestCertificate_signFailForPublicCertificate: function() {
        var pubCert = Certificate.parsePublic("AMPs7bmeIn7QUZi/Xt+Max/n7YsNWCzpcHR2cbieecB6aU3lrtitX39D2ltPaEhm+dyEGtYNUBeBkU2GrhV1kUGWAABSdRXoFs2ttEckWfs82HVAddJF0cNEupxNukQRqara/918TjAjC61zJREGQoLmNrmrpPqDbZyoNY+C0lIp");
        var data = "ABC 0123 ёпрст";

        var sign = pubCert.sign(data);

        equal(sign, false);
    },

    TestCertificate_verify: function() {
        var cert = getMyLocalCertificate();
        var data = "ABC 0123 ёпрст";
        var sign1 = cert.sign(data).toString("hex");
        var sign2 = cert.sign(data).toString("hex");
        var sign3 = "ffc36705dd4e07322523181c6372d0022b8deb6a4e5517bf32e335c505b583a49e23acef1af9cd1715c727b1f9375f5cbb17ca9868fc00e2946f8663e6fffdc7";

        equal(cert.verify(data, sign1), true);
        equal(cert.verify(data, sign1), true);
        equal(cert.verify(data, sign2), true);
        equal(cert.verify(data, sign3), true);
        equal(cert.verify(data + ".", sign1), false);
        equal(cert.verify(data + "\n", sign2), false);
        equal(cert.verify(data + "\x00", sign3), false);
    },

    TestCertificate_isRegistrar: function() {
        var certAny       = Certificate.parsePublic("ABVA7zkrzzSzYYUt/sOp4rL3wcBl8Wg5ghHaNefu056HMtsVT54Q8QapYuvg9W0BuW0P/Pb/jZcEZ/xFfoKEUL4=");
        var certRegistrar = Certificate.parsePublic("AA3Evvx1ARDVVyiPWZ1197l3MAOPe2U1FR7hjxQpL6TQmqf8dxEEoB/AV19Um49u9U7Lb8Lx9ujX2MurTd6qnWw=");

        equal(certAny.isRegistrar(), false);
        equal(certRegistrar.isRegistrar(), true);
    },

    TestCertificate_isRegistered: function() {
        var certAnonymous  = Certificate.parsePublic("ABVA7zkrzzSzYYUt/sOp4rL3wcBl8Wg5ghHaNefu056HMtsVT54Q8QapYuvg9W0BuW0P/Pb/jZcEZ/xFfoKEUL4=");
        var certRegistered = Certificate.parsePublic("AMPs7bmeIn7QUZi/Xt+Max/n7YsNWCzpcHR2cbieecB6aU3lrtitX39D2ltPaEhm+dyEGtYNUBeBkU2GrhV1kUGWAABSdRXoFs2ttEckWfs82HVAddJF0cNEupxNukQRqara/918TjAjC61zJREGQoLmNrmrpPqDbZyoNY+C0lIp");

        equal(certAnonymous.isSignedByRootRegistrar(), false);
        equal(certRegistered.isSignedByRootRegistrar(), true);
        equal(certAnonymous.rsign, "");
        equal(certRegistered.rsign, "960000527515e816cdadb4472459fb3cd8754075d245d1c344ba9c4dba4411a9aadaffdd7c4e30230bad732511064282e636b9aba4fa836d9ca8358f82d25229");
    },

    TestCertificate_getRegistrarCertificateByZone: function() {
        var zone = "base";

        var cert = Certificate.getRegistrarCertificateByZone(zone);

        equal(cert.isRegistrar(), true);
        equal(cert.toString(), "AA3Evvx1ARDVVyiPWZ1197l3MAOPe2U1FR7hjxQpL6TQmqf8dxEEoB/AV19Um49u9U7Lb8Lx9ujX2MurTd6qnWw=");
    }
});

