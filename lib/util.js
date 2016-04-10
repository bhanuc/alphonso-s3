'use strict';

let Bluebird = require('bluebird');

function concat(stream) {
  return new Bluebird(function(fulfill, reject) {
    let strings = [];
    stream.setEncoding('utf8');
    stream.on('error', reject);
    stream.on('data', function(data) {
      strings.push(data);
    });
    stream.on('end', function() {
      fulfill(strings.join(''));
    });
  });
}

exports.concat = concat;