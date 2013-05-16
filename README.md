An [AWS S3](http://aws.amazon.com/s3/) [TileSource and TileSink](https://github.com/mapbox/tilelive.js/blob/master/API.md) for use with [tilelive](https://github.com/mapbox/tilelive.js).

# Install

I'll post it to npm's registry when its more complete, for now you can use

`npm install git://github.com/pagameba/s3tiles.git`

# Configuration

You will need your AWS Account access key and secret access key.  Create a json file as follows:

```
{
  "accessKeyId":"<aws-access-key>",
  "secretAccessKey":"<aws-secret-access-key>"
}
```

Set an environment variable AWS_CREDENTIALS_FILE to the path to this file, either in your environment or as part of launching your script, i.e.

`$> AWS_CREDENTIALS_FILE=/path/to/aws-credentials.json node myscript.js` 

# Usage

See the [tilelive documentation](https://github.com/mapbox/tilelive.js) for details, but basically you can do this:

```
var tilelive = require('tilelive');
var awsOptions = {
  credentials: './aws-credentials.json',
  region: 'us-east-1'
};
require('s3tiles')(awsOptions).registerProtocols(tilelive);
```

You can also use it with the tilelive copy command, for instance the following command will transfer all the tiles from the /data/tiles.mbtiles sqlite database to an S3 bucket called `my-data` inside a folder called `tiles` and mark all the tiles with content-type of image/png.

``$> AWS_CREDENTIALS_FILE=/path/to/aws-credentials.json AWS_REGION=us-east-1 node_modules/tilelive/bin/copy mbtiles:///data/tiles.mbtiles s3tiles://my-data/tiles#content-type=image/png --scheme pyramid`

# URI Format

`tilelive` sources and sinks are specified using a URI.  The protocol for S3Tiles is 's3tiles:'.  The URI is parsed using node's `url.parse` method.

# Options

