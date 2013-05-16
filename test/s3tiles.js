var assert = require('better-assert')
  , uuid = require('uuid')
  , fs = require('fs')
  , path = require('path')
  , aws = require('aws-sdk');
  
beforeEach(function() {
  if (!process.env.AWS_CREDENTIALS_FILE) {
    throw new Error('Missing AWS credentials file');
  }
});

describe('S3Tiles', function() {
  var S3Tiles = require('../');
  
  var testBucket = 's3tiles-test-bucket'; // uuid.v4()
  var testTileset = 'tiles';
  
  describe('putTile', function() {
    it('should put a tile', function(done) {
      new S3Tiles('s3tiles://'+testBucket+'/'+testTileset, function(err, sink) {
        sink.startWriting(function() {
          fs.readFile(path.join(__dirname, 'fixtures/images/plain_1_0_0_0.png'), function(err, buffer) {
            sink.putTile(0,0,0,buffer, done);
          });
        })
      });
    });
  });
  
  describe('getTile', function() {
    it('should get a tile', function(done) {
      this.timeout(6000);
      
      new S3Tiles('s3tiles://'+testBucket+'/'+testTileset, function(err, sink) {
        if (err) return done(new Error(JSON.stringify(err)));
        sink.getTile(0,0,0,function(err, data) {
          if (err) return done(new Error(JSON.stringify(err)));
          fs.readFile(path.join(__dirname, 'fixtures/images/plain_1_0_0_0.png'), function(err, buffer) {
            if (err) return done(new Error(JSON.stringify(err)));
            assert(data.toString('base64') == buffer.toString('base64'));
            done();
          });
        });
      });
    })
  })
});