var NATS = require('nats');

var servers = ['nats://172.16.61.20:4222'];
var nats = NATS.connect({'servers': servers});

// currentServer is the URL of the connected server.
console.log("Connected to " + nats.currentServer.url.host);

// Simple Subscriber
nats.subscribe('publish-data', function(msg) {
  console.log('Received a message: ' + msg);
});
