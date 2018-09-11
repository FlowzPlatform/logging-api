const { json, send } = require('micro')
//const _ = require('lodash')
const cors = require('micro-cors')()

var self = {

    publishNats: cors(async(req, res) => {
        try {

          console.log('inside nats..');
          var NATS = require('nats');

          var servers = ['nats://172.16.61.20:4222'];
          var nats = NATS.connect({'servers': servers});

          // currentServer is the URL of the connected server.
          console.log("Connected to " + nats.currentServer.url.host);

          let jsonData = await json(req);

          // Simple Publisher
          nats.publish('publish-data', JSON.stringify(jsonData));

          send(res, 200, jsonData)

        } catch(e) {
          console.log('Exc :: ', e);
        }


      })
    }

module.exports = self;
