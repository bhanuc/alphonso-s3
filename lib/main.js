'use strict';
/*jshint -W079 */
let Promise = require('bluebird');
let config;
let util = require('./util');

let s3 = require('knox').createClient(config.s3);
s3 = Promise.promisifyAll(s3);

function stream(key) {
  return s3.getFileAsync(key)
    .then(function(res) {
      if (res.statusCode === 404) {
        return;
      }
      if (res.statusCode !== 200) {
        util.concat(res)
          .then(function(err) {
            throw new Error('Error downloading ' + key + '\n' + err);
          });
      }
      return res;
    });
}

function download(key) {
  return stream(key)
    .then(function(res) {
      if (!res) {
        return;
      }
      return util.concat(res);
    });
}


s3.stream = stream;
s3.download = download;

s3.writePackage = function(key, data, metadata, type) {
  if (type == "buffer") {
    return s3.putBufferAsync(data, key, metadata);
  }
  else if (type == "stream") {
    return s3.putStreamAsync(data, key, metadata);
  }
};
s3.writeContent = function(key, data) {
  return s3.putBufferAsync(data, key);
};


s3.fileExists = stream;
s3.streamFile = stream;

module.exports = function(conf) {
  config = conf;
  return s3;
};
