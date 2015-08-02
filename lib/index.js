var Server = require("./Server");

module.exports = {

    Server: Server,

    startServer: function(argv) {
        return new Server(argv);
    }
};