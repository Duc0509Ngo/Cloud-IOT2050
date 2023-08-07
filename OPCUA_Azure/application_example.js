var opcua = require("node-opcua");
var async = require("async");

var clientFromConnectionString = require('azure-iot-device-amqp').clientFromConnectionString;
var Message = require('azure-iot-device').Message;

var client = new opcua.OPCUAClient();

var endpointUrl = "opc.tcp://192.168.200.10:4840"; //change

var the_session = null;


var connectionString = 'HostName={Hostname};DeviceId={DeviceID};SharedAccessKey={DeviceKey}'; //change
//ie. var connectionString = 'HostName=OPC-Broker.azure-devices.net;DeviceId=anyname;SharedAccessKey=8i5+tXZCzaR77jnoW0YxYO3L9e7GCHbAVDnq3sh3rT70=';
var aclient = clientFromConnectionString(connectionString);



var opc_items = new Array();
var items_idx = 0;
var monitored_items = new Array();
var opc_start = 'ns=3;s="Browse_Startpoint"'; //change
var newDP = null;

async.series([
    // step 1a : connect to OPC-UA server
    function (callback) {
        client.connect(endpointUrl, function (err) {
            if (err) {
                console.log("OPC-UA: cannot connect to endpoint:", endpointUrl);
            } else {
                console.log("OPC-UA: connected");
            }
            callback(err);
        });
    },
    // step 1b : connect to Azure IoT Hub
    function (callback) {
        aclient.open(function (err) {
            if (err) {
                console.log('IoT Hub: could not connect: ' + err);
            } else {
                console.log('IoT Hub: client connected');
            }
            callback(err);
        });
    },
    // step 2 : createSession
    function (callback) {
        client.createSession(function (err, session) {
            if (!err) {
                the_session = session;
            }
            callback(err);
        });
    },
    // step 3 : browse
    function (callback) {
        the_session.browse(opc_start, function (err, browse_result, diagnostics) {
            if (!err) {
                browse_result[0].references.forEach(function (reference) {
                    if (reference.nodeId.namespace == 0) {
                        newDP = "i=" + reference.nodeId.value;
                    }
                    else {
                        newDP = "ns=" + reference.nodeId.namespace + ";s=" + reference.nodeId.value;
                        opc_items[items_idx] = newDP;
                        items_idx = items_idx + 1;
                    }
                    console.log(newDP);
                });
            }
            callback(err);
        });
    },
    // step 4 : install subscriptions to monitor items and send data to azure
    //
    // create subscription
    function (callback) {
        the_subscription = new opcua.ClientSubscription(the_session, {
            requestedPublishingInterval: 1000,
            requestedLifetimeCount: 10,
            requestedMaxKeepAliveCount: 2,
            maxNotificationsPerPublish: 10,
            publishingEnabled: true,
            priority: 10
        });
        the_subscription.on("started", function () {
            console.log("subscription started - subscriptionId=", the_subscription.subscriptionId);
        }).on("internal_error", function (err) {
            console.log("internal error " + err);
        }).on("keepalive", function () {
            //console.log("keepalive");
        }).on("status_changed", function () {
            console.log("status changed");
        }).on("terminated", function () {
            console.log("terminated");
            callback();
        });
        setTimeout(function () {
            the_subscription.terminate();
            console.log("timeout");
        }, 3600000);

        // install monitored items
        items_idx = 0;
        console.log("Len " + opc_items.length);
        opc_items.forEach(function (opc_item) {
            monitored_items[items_idx] = the_subscription.monitor({
                nodeId: opcua.resolveNodeId(opc_item),
                attributeId: 13
            },
            {
                samplingInterval: 100,
                discardOldest: true,
                queueSize: 10
            });

            monitored_items[items_idx].on("changed", function (value) {
                var date = new Date();
                var opc_item_ = opc_item.replace(opc_start, '');
                opc_item_ = opc_item_.replace('.', '');
                opc_item_ = opc_item_.replace('\"', '');
                opc_item_ = opc_item_.replace('\"', '');
                var jsonstrg = '{ timecreated: ' + date.toISOString() + ', deviceId: iot2000, ' + opc_item_ + ': ' + value.value.value + ' }'; //change deviceID if needed
                var data = JSON.stringify(jsonstrg);
                var message = new Message(data);
                console.log(opc_item + " sending message: " + message.getData());

                aclient.sendEvent(message, function (op) {
                    return function printResult(err, res) {
                        if (err) console.log(op + ' error: ' + err.toString());
                        if (res) console.log(op + ' status: ' + res.constructor.name);
                    };
                });
            });

            items_idx = items_idx + 1;
        });
    },

    // closing session
    function (callback) {
        console.log(" closing session");
        the_session.close(function (err) {
            console.log(" session closed");
            callback();
        });
    },
],
    function (err) {
        if (err) {
            console.log(" failure ", err);
        } else {
            console.log(" done!")
        }
        client.disconnect(function () { });
    }
);
