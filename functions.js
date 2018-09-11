const { json, send } = require('micro')
//const _ = require('lodash')
const cors = require('micro-cors')()

var self = {

    publishRequest: cors(async(req, res) => {
        try {
          console.log('aaa');

          let jsonData = await json(req);

          /* kafka publish */
          var kafka = require('kafka-node');
          var HighLevelProducer = kafka.HighLevelProducer;
          var KeyedMessage = kafka.KeyedMessage;
          var Client = kafka.Client;

          var client = new Client('localhost:2181', 'my-client-id', {
            sessionTimeout: 300,
            spinDelay: 100,
            retries: 2
          });



          // For this demo we just log client errors to the console.
          client.on('error', function(error) {
            console.error(error);
          });

          var prod_options = {
              // Configuration for when to consider a message as acknowledged, default 1
              requireAcks: 1,
              // The amount of time in milliseconds to wait for all acks before considered, default 100ms
              ackTimeoutMs: 100,
              // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3, custom = 4), default 2
              partitionerType: 0
          }
          var producer = new HighLevelProducer(client,prod_options);

          producer.on('ready', function() {
            // Create message and encode to Avro buffer

            /*
            var messageBuffer = type.toBuffer({
              enumField: 'sym1',
              id: '3e0c63c4-956a-4378-8a6d-2de636d191de',
              timestamp: Date.now()
            }); */
/*
          var jsonData = {
          	"name": "kafka",
          	"type": "produccer"
          };
*/
            const messageBuffer = new Buffer.from(JSON.stringify(jsonData));

            // Create a new payload
            var payload = [{
              topic: 'test-123',
              messages: messageBuffer,
              partition: 0,
              //partition: 1,
              attributes: 1 /* Use GZip compression for the payload */
            }];

            // var payload2 = [{
            //   topic: 'node-data',
            //   messages: messageBuffer,
            //   partition: 1,
            //   //partition: 1,
            //   attributes: 1 /* Use GZip compression for the payload */
            // }];

            //Send payload to Kafka and log result/error
            producer.send(payload, function(error, result) {
              console.info('Sent payload to Kafka: ', payload);
              if (error) {
                console.error(error);
              } else {
                var formattedResult = result[0];
                console.log('result: ', result)
              }
            });

            // producer.send(payload2, function(error, result) {
            //   console.info('Sent payload to Kafka: ', payload);
            //   if (error) {
            //     console.error(error);
            //   } else {
            //     var formattedResult = result[0];
            //     console.log('result: ', result)
            //   }
            // });

          });

          // For this demo we just log producer errors to the console.
          producer.on('error', function(error) {
            console.error(error);
          });

          /* kafka pulish */

          send(res, 200, jsonData)

        } catch(e) {
          console.log('Exc :: ', e);
        }


      })
    }

module.exports = self;
