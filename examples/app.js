
const AWS = require('aws-sdk');
const s3Downloader = require('../s3MultipartDownloader');
const s3Params = require('../s3params');

AWS.config.loadFromPath('../awsconfig.json');

const s3 = new AWS.S3(s3Params);
const fname = 'package.json';

const options = {
    s3Obj : s3,
    s3ApiVersion : s3Params.apiVersion,
    s3Bucket : s3Params.Bucket,
    s3Key : fname,
    partSize : 10 * 1024 * 1024
}

const downloader = new s3Downloader(options);
downloader.start(5);
