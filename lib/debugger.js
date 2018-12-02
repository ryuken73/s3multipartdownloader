const debug = require('debug');
const debugProgress = debug('s3downloader:progress');
const debugStart = debug('s3downloader:start');
const debugEvent = debug('s3downloader:event');
const debugS3Result = debug('s3downloader:s3');
const debugJobRunner = debug('jobRunner:app')

module.exports = {
    debugProgress, debugStart, debugEvent, debugS3Result, debugJobRunner
}