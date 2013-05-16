var aws = require('aws-sdk')
  , util = require('util')
  , fs = require('fs')
  , path = require('path')
  , url = require('url')
  , qs = require('querystring')
  , s3
  ;

var options = {
  credentials: path.resolve(process.env.AWS_CREDENTIALS_FILE || './aws-credentials.json'),
  region: process.env.AWS_REGION || 'us-east-1'
};

if (!fs.existsSync(options.credentials)) {
  throw new Error ('AWS credentails file not found: %s', options.credentials);
}

aws.config.loadFromPath(options.credentials);
aws.config.update({region:options.region});

s3 = new aws.S3();

exports = module.exports = S3Tiles;

function S3Tiles(uri, callback) {
  if (typeof uri === 'string') {
    uri = url.parse(uri, true);
  } else if (typeof uri.query === 'string') {
    uri.query = qs.parse(uri.query);
  }
  
  this._isWriting = 0;
  this.contentType = 'image/jpeg';

  if (uri.hash) {
    this.contentType = uri.hash.split('#')[1];
  } else {
    console.log('warning: no content-type specified, defaulting to %s.', this.contentType);
  }
  
  var bucket = this.bucket = uri.host;
  this.tileset = uri.path.split('/')[1];
  
  var that = this;
  s3.headBucket({"Bucket":bucket})
    .on('success', function(response) {
      callback(null, that);
    })
    .on('error', function(err) {
      s3.createBucket({"Bucket":bucket})
        .on('success', function(response) { 
          callback(null, that);
        })
        .on('error', function(response) {
          callback(new Error(util.format('error creating bucket %s', JSON.stringify(response))));
        })
        .send();    
    })
    .send();
}

S3Tiles.registerProtocols = function(tilelive) {
  tilelive.protocols['s3tiles:'] = S3Tiles;
};

S3Tiles.prototype.getTile = function(z, x, y, callback) {
  if (typeof callback !== 'function') throw new Error('Callback needed');
  s3.getObject({
      Bucket: this.bucket,
      Key: util.format('%s/%s/%s/%s', this.tileset, z, x, y),
    })
    .on('success', function(response) {
      var options = {
          'Content-Type': this.getMimeType(response.data.Body),
          'Last-Modified': response.data.LastModified,
          'ETag': response.data.ETag
      };
      callback(null, response.data.Body, options);
    })
    .on('error', function(err) {
      return callback((new Error(err)));
    })
    .send();
}

S3Tiles.prototype.getGrid = function(z, x, y, callback) {
  s3.getObject({
      Bucket: this.bucket,
      Key: util.format('%s/%s/%s/%s', this.tileset, z, x, y),
    })
    .on('success', function(err, data) {
      if (err) {
        return callback(new Error(err));
      }
      callback(null, data.Buffer);
    })
    .on('error', function(err) {
      return callback((new Error(err)));
    })
    .send();
}

S3Tiles.prototype.getInfo = function(callback) {
  callback(null, {
    bounds: [-180, -90, 180, 90]
  });
}

S3Tiles.prototype.startWriting = function(callback) {
  if (typeof callback !== 'function') throw new Error('Callback needed');
  this._isWriting ++;
  callback(null);
}

S3Tiles.prototype.stopWriting = function(callback) {
  if (typeof callback !== 'function') throw new Error('Callback needed');
  this._isWriting --;
  callback(null);
}

S3Tiles.prototype.putInfo = function(info, callback) {
  if (typeof callback !== 'function') throw new Error('Callback needed');
  callback(null);
}

S3Tiles.prototype.putTile = function(z, x, y, tile, callback) {
  if (typeof callback !== 'function') throw new Error('Callback needed');
  if (!this._isWriting) return callback(new Error('S3Tiles not in write mode'));
  if (!Buffer.isBuffer(tile)) return callback(new Error('Image needs to be a Buffer'));

  try {
    s3.putObject({
      Body: tile,
      Bucket: this.bucket,
      Key: util.format('%s/%s/%s/%s', this.tileset, z, x, y),
      ContentType: this.contentType,
      ACL: 'public-read'
    }, function(err, data) {
      if (err) {
        return callback(err)
      }
      callback(null);
    });
  } catch(err) {
    console.log('S3 Exception not handled: %s', util.inspect(err));
    console.log('Retrying operation in 1 second');
    
    var that = this;
    setTimeout(function() {
      that.putTile(z, x, y, tile, callback);
    }, 1000);
  }
}

S3Tiles.prototype.putGrid = function(z, x, y, grid, callback) {
  if (typeof callback !== 'function') throw new Error('Callback needed');
  callback(null);
}

S3Tiles.prototype.close = function(callback) {
  callback(null);
}

S3Tiles.prototype.getMimeType =  function(data) {
  if (data[0] === 0x89 && data[1] === 0x50 && data[2] === 0x4E &&
    data[3] === 0x47 && data[4] === 0x0D && data[5] === 0x0A &&
    data[6] === 0x1A && data[7] === 0x0A) {
    return 'image/png';
  } else if (data[0] === 0xFF && data[1] === 0xD8 &&
    data[data.length - 2] === 0xFF && data[data.length - 1] === 0xD9) {
    return 'image/jpeg';
  } else if (data[0] === 0x47 && data[1] === 0x49 && data[2] === 0x46 &&
    data[3] === 0x38 && (data[4] === 0x39 || data[4] === 0x37) &&
    data[5] === 0x61) {
    return 'image/gif';
  }
};

