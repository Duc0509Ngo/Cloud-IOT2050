'use strict';
var iothub = require('azure-iothub');

var connectionString = '{iothub connection string}';							//change
var registry = iothub.Registry.fromConnectionString(connectionString);		

var device = new iothub.Device(null);
device.deviceId = 'iot2000';												//change if needed
registry.create(device, function(err, deviceInfo, res) {
  if (err) {
    registry.get(device.deviceId, printDeviceInfo);
  }
  if (deviceInfo) {
    printDeviceInfo(err, deviceInfo, res)
  }
});

function printDeviceInfo(err, deviceInfo, res) {
  if (deviceInfo) {
    console.log('Device id: ' + deviceInfo.deviceId);
    console.log('Device key: ' + deviceInfo.authentication.SymmetricKey.primaryKey);
  }
}
