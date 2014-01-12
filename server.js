
/**
 * Module dependencies.
 */

var express = require('express'),
    routes  = require('./routes'),
    Logger = require('./Logger'),
    _ = require('underscore');

var kafka = require('./kafka');
var Consumer = kafka.Consumer;
var Producer = kafka.Producer;
var Offset = kafka.Offset;
var Client = kafka.Client;

var app = module.exports = express.createServer();

// Configuration

// command line parameters
var argv = require('optimist')
    .usage('Usage: $0 [options]')
    .describe({
        host: 'Zookeeper host',
        port: 'Zookeeper port',
        topic: 'Kafka topic',
        group: 'Kafka consumer group',
        loglevel: 'Log verbosity',
        logfile: 'Log file path'
    })
    .default({
        host: 'localhost',
        port: 2181,
        topic: '52a06cb43004e8f6801f3df05287676030049e3d496ff162',
        group: 'default',
        loglevel: 'debug',
        logfile: 'Kafkazoo.log'
    })
    .argv;

// go log
var log = new Logger(argv.loglevel, argv.logfile);

app.configure(function(){
  app.set('views', __dirname + '/views');
  app.set('view engine', 'jade');
  app.use(express.bodyParser());
  app.use(express.methodOverride());
  app.use(app.router);
  app.use(express.static(__dirname + '/public'));
});

app.configure('development', function(){
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true })); 
});

app.configure('production', function(){
  app.use(express.errorHandler()); 
});

// Routes

app.get('/', function(req, res){
  res.render('index');
});

app.get('/update-stream/:topic_name/:group_name', function(req, res) {

  // let request last as long as possible
  req.socket.setTimeout(Infinity);
  var topic_name = req.params.topic_name;
  var group_name = req.params.group_name;

  var client = new Client();
  var topics = [
            {topic: topic_name},
        ],
       options = {
           host: 'localhost',
           autoCommit: false,
           fromBeginning: false,
           fetchMaxWaitMs: 1000,
           fetchMaxBytes: 1024*1024
       };

  var consumer = new Consumer(client, topics, options);

  function createConsumer(consumer, topics) {

        var offset = new Offset(client);

        consumer.on('message', function (message) {
            console.log(this.id, message);
            // el - current element, i - index
            res.write('id: ' + this.id+'\n');
            res.write("data: " + message + '\n\n'); // Note the extra newline
        });

        consumer.on('error', function (err) {
            console.log('error', err);
        });

        consumer.on('offsetOutOfRange', function (topic) {
            topic.maxNum = 2;
            offset.fetch([topic], function (err, offsets) {
                var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
                consumer.setOffset(topic.topic, topic.partition, min);
            });
        })
  }

  createConsumer(consumer, topics);

  //send headers for event-stream connection
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });
  res.write('\n');

  // The 'close' event is fired when a user closes their browser window.
  // In that situation we want to make sure our redis channel subscription
  // is properly shut down to prevent memory leaks...and incorrect subscriber
  // counts to the channel.
  req.on("close", function() {
     // consumer.stopConsuming();
     // consumer.destroy();
  });
});
// http://localhost:8001/fire-event/TEST/52a06cb43004e8f6801f3df05287676030049e3d496ff162/target/caller
app.get('/fire-event/:event_name/:topic_name/:target_name/:caller_name', function(req, res) {

    var event_name = req.params.event_name;
    var topic_name = req.params.topic_name;
    var target_name = req.params.target_name;
    var caller_name = req.params.caller_name;

    var client = new Client();

    var topics = [
            {topic: topic_name}
        ],
        options = {
            host: 'localhost',
            autoCommit: true,
            fromBeginning: false,
            fetchMaxWaitMs: 1000,
            fetchMaxBytes: 1024*1024
        };

    var producer = new Producer(client, topics, options);

    var count = 1, rets = 0;
    producer.on('ready', function () {
        console.log("producing for ", producer.topic);
        //setInterval(send, 1000);
        send(createMessage());
    });

    function createMessage () {
        var idz = (Math.floor(Math.random() * Math.pow(2, 2) * 10 + 5).toString());
        var message = {
            "index" : producer.topic,
            "type"  : "kafka",
            "id"    : idz.toString(),
            "source" : {
                "topic" : topic_name,
                "message" : event_name,
                "target"  : target_name,
                "caller"  : caller_name,
                "location": [22,23],
                "date"    : new Date()
            }
        };
        return JSON.stringify(message);
    }

    function send(message) {
        message = message || createMessage();
        for (var i = 0; i < count; i++) {
            producer.send([
                {topic: producer.topic, messages: [message] }
            ], function (err, data) {
                if (err) console.log(arguments);
                if (++rets === count) process.exit();
            });
        }
    }

    producer.on('error', function(err){
        console.log("some general error occurred: ", err);
    });
    producer.on('brokerReconnectError', function(err){
        console.log("could not reconnect: ", err);
        console.log("will retry on next send()");
    });

  res.writeHead(200, {'Content-Type': 'text/html'});
  res.write('All clients have received "' + req.params.event_name + '"');
  res.end();
});

app.listen(8001);
console.log("Express server listening on port %d in %s mode", app.address().port, app.settings.env);
