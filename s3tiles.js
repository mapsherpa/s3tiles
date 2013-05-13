var aws = require('aws-sdk')
  , util = require('util')
  , url = require('url')
  , qs = require('querystring')
  , s3
  ;

if (process.env.AWS_CREDENTIALS_FILE) {
  aws.config.loadFromPath(process.env.AWS_CREDENTIALS_FILE);
} else {
  aws.config.loadFromPath('./aws-credentials.json');
}

aws.config.update({region:'us-east-1'});
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
  
  var parts = uri.path.split('/');
  if (parts.length !== 2) {
    throw new Error('Invalid uri, must be formatted as s3tiles://<bucket>/<tileset>#<content-type>');
  }
  if (uri.hash) {
    this.contentType = uri.hash.split('#')[1];
  } else {
    console.log('warning: no content-type specified, defaulting to %s.', this.contentType);
  }
  
  var bucket = this.bucket = parts[0];
  this.tileset = parts[1];
  
  var that = this;
  s3.headBucket({"Bucket":bucket}, function(err, data) {
    if (err) {
      console.log('Bucket ' + bucket + ' missing, creating bucket (%s)', util.inspect(err));
      s3.createBucket({"Bucket":bucket})
        .on('success', function(response) { 
          console.log('bucket created.');
          callback(null, that);
        })
        .on('error', function(response) {
          callback(new Error(util.format('error creating bucket %s', JSON.stringify(response))));
        })
        .send();    
    } else {
      callback(null, that);
    }
  });
}

S3Tiles.registerProtocols = function(tilelive) {
  tilelive.protocols['s3tiles:'] = S3Tiles;
};

S3Tiles.prototype.getTile = function(z, x, y, callback) {
  callback(new Error('getTile not implemented'));
}

S3Tiles.prototype.getGrid = function(z, x, y, callback) {
  callback(new Error('getGrid not implemented'));
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
  
}

S3Tiles.prototype.putGrid = function(z, x, y, grid, callback) {
  if (typeof callback !== 'function') throw new Error('Callback needed');
  callback(null);
}

S3Tiles.prototype.close = function(callback) {
  callback(null);
}
