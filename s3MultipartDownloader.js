const fs = require('fs');
const path = require('path');
const ParallelJobQueue = require('./lib/jobRunner');
const eventEmitter = require('events');
const {debugProcess, debugStart, debugEvent, debugS3Result} = require('./lib/debugger')


class s3Downloader extends eventEmitter {

    constructor(options) {

        super();
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
        this.TASK.partCounts = undefined;
        this.TASK.fileName = options.fileName || options.s3Key;
        this.TASK.fileSzie = undefined;
        this.TASK.concurrency = options.concurrency || this.MAX_CONCURRENT;
        // filename : srcName = fileName, dstName = targetDirectory + srcName
        this.TASK.srcName = undefined;
        this.TASK.dstName = undefined;
        // multipart slice info
        // [{partNum:, start:, end:, size:, status:}...]
        this.TASK.rangeInfo = [];
        // used to calculate last speed and estimated end time 
        // {loaed: previous total loaded, eventTime: previous event time}
        this.TASK.lastProgress = {}; 
        //save s3headObject to use metadata
        this.TASK.S3headInfo = undefined; 
        this.TASK.TIMEINFO = {startTime : undefined, endTime : undefined};

        // keep part's progress info
        // [{partNum: {loaded: last loaded size, total: part Size, startTime: date, endTime: date}}]
        this.PART = {};
        this.PART.PROGRESS = {};

        this.logger = global.logger ? global.logger : console;
        this.logCollector = options.logCollector;
    }

    _s3GetHeadInfo(){
        return new Promise((resolve,reject) => {
            try {
                this.s3.headObject(this.s3params, (err,data) => {
                    if(err) reject(err);
                    debugS3Result(data);
                    if(data){
                        this.TASK.S3headInfo = data;
                        this.TASK.fileSize = data.ContentLength;
                        resolve(true)
                    } else {
                        reject(`S3 headObject Failed. Not exist [Key:${this.s3params.Key}]`)
                    }    
                    
                })        
            } catch (err) {
                console.log(err);
            }
        })
    }

    _setPartCount(){
        const partCounts = Math.ceil(this.TASK.fileSize / this.TASK.partSize);
        if(partCounts > this.MAX_PARTCOUNT) {
            // File size too Large
            // Adjust partSize using max_partcount
            this.TASK.partSize = Math.ceil(this.TASK.fileSize / this.MAX_PARTCOUNT);
            this._setPartCount()
        }
        this.TASK.partCounts = partCounts;
        return
    }

    _setRangeInfo(){
        let partNum = 1;
        let partCounts = this.TASK.partCounts;
        let start = 0;
        while(partCounts > 0){
            let slice = {};
            slice.partNum = partNum;
            slice.start = start;
            if(partCounts === 1 || (slice.end > this.TASK.fileSize)){
                slice.end = this.TASK.fileSize;                
            } else {
                slice.end = start + this.TASK.partSize;
                start = slice.end;
            }
            slice.size = slice.end - slice.start;
            slice.status = 'ready';
            this.TASK.rangeInfo.push(slice);
            partCounts --
            partNum ++
        }
    }

    _getObject(params){
        const {slice, getObjectParams} = params;
        return new Promise((resolve,reject) => {
            const {partNum} = slice; 
            this.PART.PROGRESS[partNum] = {};
            this.PART.PROGRESS[partNum].startTime = new Date();
            const downloader = this.s3.getObject(getObjectParams, (err,data) => {
                if(err) reject(err);
                this.PART.PROGRESS[partNum].endTime = new Date();
                slice.status = 'done';
                //console.log(data);
                const partResult = {};
                partResult.partNum = partNum;
                partResult.size = slice.size;
                partResult.range = getObjectParams.Range;
                partResult.startTime = this.PART.PROGRESS[partNum].startTime;
                partResult.endTime = this.PART.PROGRESS[partNum].endTime;

                // update this.PART.PROGRESS
                this.PART.PROGRESS[partNum].loaded = partResult.size;

                this.emit('partDownloaded', partResult);
                resolve(data) 
            })      

            downloader.on('httpDownloadProgress', this._progressHandler(slice));
        })
    }

    _progressHandler(slice){
        return (progress) => {
            const eventTime = new Date();
            const {partNum} = slice;
            //console.log(progress)

            this.PART.PROGRESS[partNum].loaded = progress.loaded;
            this.PART.PROGRESS[partNum].total = progress.total;
            const progressInfo = this._getProgressInfo(eventTime)  

            const elapsedms = eventTime.getTime() - this.TASK.lastProgress.eventTime.getTime();
            const updateIntervalms = 500;
            if(elapsedms > updateIntervalms) {
                const prevLoaded = this.TASK.lastProgress.loaded;
                const prevTime = this.TASK.lastProgress.eventTime;
                progressInfo.lastLoaded = progressInfo.loaded - prevLoaded;
                progressInfo.lastLoadedMB = (progressInfo.lastLoaded / (1024 * 1024)).toFixed(1) + 'MB';
                progressInfo.lastSpeed = this._getAvgSpeed(prevTime, eventTime, progressInfo.lastLoaded );
                progressInfo.remainSec = this._getRemainSec(this.TASK.TIMEINFO.startTime, eventTime, progressInfo.loaded, progressInfo.fsize);
                progressInfo.status = 'downloading';
                this.emit('progress', progressInfo);  

                this.TASK.lastProgress.eventTime = eventTime;
                this.TASK.lastProgress.loaded = progressInfo.loaded; 
            }



            //progressInfo.status = 'downloading';
            //this.emit('progress', progressInfo);  
            //console.log(progressInfo.percentString);      
        }
    }

    _getProgressInfo(eventTime){ 
        const progressInfo = {};
        progressInfo.startTime = this.TASK.TIMEINFO.startTime; 
        progressInfo.endTime = this.TASK.TIMEINFO.endTime;
        progressInfo.srcName = this.s3params.Key;
        progressInfo.dstName = this.TASK.dstFile;
        progressInfo.fsize = this.TASK.fileSize;
        progressInfo.s3Header = this.TASK.S3headInfo.Metadata;
        progressInfo.loaded = this._getTotalLoaded();  
        progressInfo.loadedMB = (progressInfo.loaded  / (1024 * 1024)).toFixed(1) + 'MB';
        progressInfo.percent = (progressInfo.loaded / progressInfo.fsize ) * 100;
        progressInfo.percentString = progressInfo.percent.toFixed(1) + '%';
        progressInfo.partSize = this.TASK.partSize;
        progressInfo.partCounts = this.TASK.partCounts;
        progressInfo.concurrency = this.TASK.concurrency;
        progressInfo.loadedParts = 0;
        this.TASK.rangeInfo.map((slice) => {
            if (slice.status === 'done') progressInfo.loadedParts++;
        })
        progressInfo.avgSpeed = this._getAvgSpeed(this.TASK.TIMEINFO.startTime, eventTime, progressInfo.loaded)
        return progressInfo   
    }

    _getAvgSpeed(startT, endT, size) {
        const startTimestamp = startT.getTime();
        const endTimestamp = endT.getTime();
        const elapsedSec = ( endTimestamp - startTimestamp ) / 1000;
        return ( size / ( elapsedSec * 1024 * 1024 )).toFixed(1);     
    }

    _getRemainSec(startT, endT, loaded, totalSize){
        const startTimestamp = startT.getTime();
        const endTimestamp = endT.getTime();
        const elapsedSec = ( endTimestamp - startTimestamp ) / 1000;
        const lastSpeed = loaded / elapsedSec;
        const remainSize = totalSize - loaded;
        //console.log(`${startTimestamp} ${endTimestamp} ${elapsedSec} ${lastSpeed} ${totalSize} ${loaded}`)
        return (remainSize / lastSpeed).toFixed(0);
        /*
        const estimatedEndTimestamp = endTimestamp + remainSec*1000;
        return new Date(estimatedEndTimestamp);
        */

    }

    _getTotalLoaded() {
        const progresses = this.PART.PROGRESS;
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

                await this._s3GetHeadInfo();
                await this._setPartCount();
                await this._setRangeInfo();

                const options = {
                    saveJobResults : false,
                    stopOnJobFailed : true,
                }

                const jobQueue = new ParallelJobQueue((result) => {
                    //resolve(result);
                    debugStart('S3 multipart Read Done, some chunk might not be written yet!')
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

                this.TASK.TIMEINFO.startTime = new Date();
                debugStart(`download parameters : [concurrency:${parallel}][partSize:${this.TASK.partSize}][partCounts:${this.TASK.rangeInfo.length}]`)

                this.TASK.lastProgress.eventTime = new Date();

                jobQueue.start(parallel);
                
                debugStart('jobQueue Started');

                jobQueue.on('jobDone', (downloadedPart, job) => {
                    const jobNum = job.jobNum;
                    const partNum = job.args.slice.partNum;
                    const offset = job.args.slice.start;
                    
                    this._writePartToFile(writeFD, offset, downloadedPart)                   
                    .then(() => {
                        writeToFileDone.push(jobNum);
                        debugStart(`write done ${partNum}`);
                        if(writeToFileDone.length === this.TASK.rangeInfo.length){
                            debugStart('All Write Done');
                            fs.closeSync(writeFD);
                            resolve('All chunk write done');
                            const eventTime = new Date();                            
                            const progressInfo = this._getProgressInfo(eventTime)
                            progressInfo.endTime = eventTime;
                            progressInfo.status = 'done';
                            this.emit('progress', progressInfo);  
                        }
                    })
                })

            } catch (err) {
                debugStart(err.code);
                reject(err);
            }



        })
    }

}

module.exports = s3Downloader;




