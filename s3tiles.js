var util = require('util')
  , fs = require('fs')
  , path = require('path')
  , url = require('url')
  , qs = require('querystring')
  , s3
  ;

const { S3Client, CreateBucketCommand, HeadBucketCommand, GetObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");
const { fromIni } = require("@aws-sdk/credential-providers");

var options = {
  region: process.env.AWS_REGION || 'us-east-1'
};

s3 = new S3Client({
  credentials: fromIni(),
  region: options.region
});

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
  }
  
  var bucket = this.bucket = uri.host;
  this.tileset = uri.path.split('/')[1];
  
  var that = this;

  const params = {"Bucket":bucket};
  const command = new HeadBucketCommand(params);

  s3.send(command)
  .then( response => {
    callback(null, that);
  })
  .catch( err => {
    const command = new CreateBucketCommand(params);
    
    s3.send(command)
    .then ( response => {
      callback(null, that);
    })
    .catch( err => {
      callback(new Error(util.format('error creating bucket %s', JSON.stringify(response))));
    });
  });
}

S3Tiles.registerProtocols = function(tilelive) {
  tilelive.protocols['s3tiles:'] = S3Tiles;
};

S3Tiles.prototype.getTile = function(z, x, y, callback) {
  if (typeof callback !== 'function') throw new Error('Callback needed');
  var that = this;

  const params = {
    Bucket: this.bucket,
    Key: util.format('%s/%s/%s/%s', this.tileset, z, x, y),
  };

  const command = new GetObjectCommand (params);
  
  s3.send(command)
  .then ( response => {
    const options = {
        'Content-Type': response.ContentType.replace("content-type=",""),
        'Last-Modified': response.LastModified,
        'ETag': response.ETag.replace('"','')
    };

    streamToBuffer(response.Body)
    .then( buffer => {
      return callback(null, buffer, options);
    })
    .catch (err => {
      return callback(err);
    })
  })
  .catch ( err => {
    if (err.name && err.name != 'NoSuchKey'){
      return callback(err);
    }
  });
}

S3Tiles.prototype.getGrid = function(z, x, y, callback) {
  const params = {
    Bucket: this.bucket,
    Key: util.format('%s/%s/%s/%s', this.tileset, z, x, y),
  };

  const command = new GetObjectCommand(params);

  s3.send(command)
  .then ( response => {
    return callback(null, streamToBuffer(response.Body));
  })
  .catch ( err => {
    return callback(err);
  });
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
  // don't think we need this
  if (typeof callback !== 'function') throw new Error('Callback needed');
  callback(null);
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

// Helper function to convert ReadableStream to Buffer using .then
function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', chunk => chunks.push(chunk));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
  });
}