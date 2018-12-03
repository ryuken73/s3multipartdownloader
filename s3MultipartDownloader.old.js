const fs = require('fs');
const path = require('path');
const ParallelJobQueue = require('./lib/jobRunner');



class s3Downloader {

    constructor(options) {

        this.MIN_PARTSIZE = options.MIN_PARTSIZE || 5 *1024 * 1024;
        this.MAX_PARTCOUNT = options.MAX_PARTCOUNT || 999;
        this.MAX_CONCURRENT = options.MAX_CONCURRENT || 10;

        this.s3 = options.s3Obj;        
        this.s3params = {
            Bucket : options.s3Bucket,
            Key : options.s3Key
        }
        
        this.TASK = {};
        this.TASK.targetDirectory = options.targetDirectory || './' ;
        this.TASK.partSize = (options.partSize && (options.partSize > this.MIN_PARTSIZE)) ? options.partSize : this.MIN_PARTSIZE;
        this.TASK.fileName = options.fileName || options.s3Key;
        this.TASK.concurrency = options.concurrency || this.MAX_CONCURRENT;
        this.TASK.rangeInfo = [];

        this.PROGRESS = {};
        this.TIMEINFO = {startTime : undefined, endTime : undefined};
    }

    _S3setHeadInfo(){
        return new Promise((resolve,reject) => {
            try {
                this.s3.headObject(this.s3params, (err,data) => {
                    if(err) reject(err);
                    this.TASK.S3headInfo = data;
                    this.TASK.fileSize = data.ContentLength;
                    resolve(true)
                })        
            } catch (err) {

            }
        })
    }

    _setPartCount(){
        const partCount = Math.ceil(this.TASK.fileSize / this.TASK.partSize);
        if(partCount > this.MAX_PARTCOUNT) {
            // File size too Large
            // Adjust partSize using max_partcount
            this.TASK.partSize = Math.ceil(this.TASK.fileSize / this.MAX_PARTCOUNT);
            this._setPartCount()
        }
        this.TASK.partCount = partCount;
        return
    }

    _setRangeInfo(){
        let partNum = 1;
        let partCount = this.TASK.partCount;
        let start = 0;
        while(partCount > 0){
            let slice = {};
            slice.partNum = partNum;
            slice.start = start;
            if(partCount === 1 || (slice.end > this.TASK.fileSize)){
                slice.end = this.TASK.fileSize;                
            } else {
                slice.end = start + this.TASK.partSize;
                start = slice.end;
            }
            slice.size = slice.end - slice.start;
            slice.status = 'ready';
            this.TASK.rangeInfo.push(slice);
            partCount --
            partNum ++
        }
    }

    _getObject(params){
        const {slice, getObjectParams} = params;
        return new Promise((resolve,reject) => {
            const downloader = this.s3.getObject(getObjectParams, (err,data) => {
                if(err) reject(err);
                slice.status = 'done';
                //console.log(data);
                resolve(data) // data accumulated in totalResult *data.Body == partSize => memory Overhead
            })      
            const {partNum} = slice;  
            this.PROGRESS[partNum] = {};
            downloader.on('httpDownloadProgress', this._progressHandler(slice));
        })
    }

    _progressHandler(slice){
        return (progress) => {
            const eventTime = new Date();
            const {partNum} = slice;
            //console.log(progress)

            this.PROGRESS[partNum].loaded = progress.loaded;
            this.PROGRESS[partNum].total = progress.total;
            const progressInfo = this._getProgressInfo(eventTime)    
            //console.log(progressInfo.percentString);      
        }
    }

    _getProgressInfo(eventTime){
        const progressInfo = {};
        progressInfo.startTime = this.TIMEINFO.startTime;
        progressInfo.endTime = this.TIMEINFO.endTime;
        progressInfo.srcName = this.s3params.Key;
        progressInfo.dstName = this.TASK.dstFile;
        progressInfo.fsize = this.TASK.fileSize;
        progressInfo.loaded = this._getTotalLoaded();  
        progressInfo.loadedMB = (progressInfo.loaded  / (1024 * 1024)).toFixed(1) + 'MB';
        progressInfo.percent = ( progressInfo.loaded / progressInfo.fsize ) * 100;
        progressInfo.percentString = progressInfo.percent.toFixed(1) + '%';
        progressInfo.partSize = this.TASK.partSize;
        progressInfo.partCount = this.TASK.partCount;
        progressInfo.concurrency = this.TASK.concurrency;
        progressInfo.loadedParts = 0;
        this.TASK.rangeInfo.map((slice) => {
            if (slice.status === 'done') progressInfo.loadedParts++;
        })
        progressInfo.avgSpeed = this._getAvgSpeed(this.TIMEINFO.startTime, eventTime, progressInfo.loaded)
        return progressInfo   
    }

    _getAvgSpeed(startT, endT, size) {
        const startTimestamp = startT.getTime();
        const endTimestamp = endT.getTime();
        const elapsedSec = ( endTimestamp - startTimestamp ) / 1000;
        return ( size / ( elapsedSec * 1024 * 1024 )).toFixed(1);     
    }

    _getTotalLoaded() {
        const progresses = this.PROGRESS;
        let totalLoaded = 0;
        for(let key in progresses) {
            const progress = progresses[key];
            const loaded = (typeof progress.loaded == 'number' && isFinite(progress.loaded)) ?  progress.loaded : 0 ;
            totalLoaded += loaded;        
        }
        return totalLoaded;
    }

    _write(writeFD, buff, dataLength, writeOffset) {
        return new Promise((resolve,reject) => {
            fs.write(writeFD, buff, 0, dataLength, writeOffset, (err,bytesWrite,outBuff) => {
                if(err) reject(err);
                resolve({success:true, bytesWrite:bytesWrite});
            })
        })
    }


    async _writePartToFile(writeFD, offset, getObjectResult) {
        try {
            const contentLength = getObjectResult.ContentLength;
            const chunkBuffer = getObjectResult.Body;
            const writeResult = await this._write(writeFD, chunkBuffer, Number(contentLength), offset);
            return writeResult;
        } catch(err) {
            console.log(err)
        }
    }
 

    _open(fname,flag){
        return new Promise((resolve,reject) => {
            fs.open(fname, flag, (err,fd) => {
                if(err) {
                    console.log(err);
                    reject(err);
                }
                resolve(fd);
            })
        })
    }

    _verifyComplete(results) {
        console.log(results);

    }

    start(parallel){
        return new Promise( async (resolve,reject) => {
            try {
                this.TASK.dstFile = path.join(this.TASK.targetDirectory, this.TASK.fileName);
                const writeFD = await this._open(this.TASK.dstFile, 'w');
                const writeToFileDone = [];

                await this._S3setHeadInfo();
                await this._setPartCount();
                await this._setRangeInfo();

                const options = {
                    saveJobResults : false,
                    stopOnJobFailed : true,
                }

                const jobQueue = new ParallelJobQueue((result) => {
                    resolve(result);
                }, options);

                this.TASK.rangeInfo.map((slice) => {
                    const rangeString = `bytes=${slice.start}-${slice.end}`
                    const params = {
                        slice : slice,
                        getObjectParams : {
                            Bucket : this.s3params.Bucket,
                            Key : this.s3params.Key,
                            Range : rangeString
                        }
                    }
                    jobQueue.addJob(this._getObject.bind(this), params);
                })

                this.TIMEINFO.startTime = new Date();
                jobQueue.start(parallel);
                
                console.log('jobQueue Started');

                jobQueue.on('jobDone', (downloadedPart, job) => {
                    const jobNum = job.jobNum;
                    const partNum = job.args.slice.partNum;
                    const offset = job.args.slice.start;
                    this._writePartToFile(writeFD, offset, downloadedPart)                   
                    .then(() => {
                        writeToFileDone.push(jobNum);
                        console.log(`write done ${partNum}`);
                        if(writeToFileDone.length === this.TASK.rangeInfo.length){
                            console.log('All Write Done');
                            fs.closeSync(writeFD);
                        }
                    })
                })

            } catch (err) {
                console.log(err);
                reject(err);
            }



        })
    }

}

module.exports = s3Downloader;




