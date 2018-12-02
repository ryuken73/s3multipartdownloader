/*
Usage 

1) Make Job Function as Promise
   * args can be object, string or array 
   function job(args, callback){       
       const {x,y,z} = args;
       return new Promise(...{

       })
   }
f
2) Set new ParallelJobQueue
   @lastCallback : callback function called when all jobs done 
   @saveJobResult (true/false) : default false
                  whether accumulate all promised job's result on totalResult Array
                  "true" can make out of memory
                  "false" convert job's result small object({jobNum:n, success:true}) and
                  push to totalResult Array ( no OOM )
   @stopOnJobFailed (true/false) : default true
   const jobQueue = new ParallelJobQueue(lastCallback, saveJobResult=false, stopOnError=false)

3) Add job function to jobQueue
   @job : job function
   @args : argument for job (object), if you need multiple argument, use object;
   
   jobQueue.addJob(job, args) 
   
   * if job function is class method and need access "this", use bind   
   jobQueue.addJob(job.bind(this), args) 


4) at last, run job with concurrency constant
   jobQueue.start(5)  

5) events
   jobQueue.on('jobDone', (jobResult, jobObj) => {})
   jobQueue.on('jobError', (error, jobObj) => {})
*/

const {debugJobRunner} = require('./debugger');

const EventEmitter = require('events');

class Job {   

    constructor(jobFunction, args, jobNum, jobTimeOutSec){
        this.jobFunction = jobFunction;
        this.args = args;
        this.jobNum = jobNum;
        this.running = false;
        this.paused = false;
        this.success = undefined;
        this.jobTimeOutSec = jobTimeOutSec ? jobTimeOutSec : 3600;
        //this.logger = global.logger ? global.logger : console;
    }

    start() {
        // return Promise
        // return this.jobFunction(this.args);
        const timerPromise = new Promise((resolve,reject) => {          
            //this.timer = setTimeout(reject, this.jobTimeOutSec * 1000, {code : 'TIMEOUT', message : `execute too long : over timeout ${this.jobTimeOutSec} sec`})
            this.timer = setTimeout(() => {
                //console.log('timeout occurred')
                debugJobRunner('timeout occurred');
                reject({code : 'TIMEOUT', message : `execute too long : over timeout ${this.jobTimeOutSec} sec`});
            },this.jobTimeOutSec * 1000)
        })
        return Promise.race([this.jobFunction(this.args), timerPromise]);
    }

    setRunning(){
        this.running = true;
    }
    setResolved(){        
        this.running = false;
        this.success = true;
    }
    setRejected(){        
        this.running = false;
        this.success = false;
    }
    isRunning(){
        return this.running;
    }
}


class ParallelJobQueue extends EventEmitter {

    constructor(lastCallback, options){
        super();
        this.jobLength = 0;
        this.jobsToRun = [];
        this.jobsRunning = [];
        this.lastCallback = lastCallback;   
        this.totalResults = [];  
        this.paused = false;
        this.cancelled = false;
        this.logger = global.logger ? global.logger : console;
        if (options && typeof(options) === 'object') {
            this.saveJobResults = options.hasOwnProperty('saveJobResults') ? options.saveJobResults : false;
            this.stopOnJobFailed = options.hasOwnProperty('stopOnJobFailed') ? options.stopOnJobFailed : true;
         } else {
            this.saveJobResults = false;
            this.stopOnJobFailed = true;
        }
    }

    addJob(jobFunction, args, jobTimeOutSec){
        try {
            const jobNum = this.jobLength + 1;
            const job = new Job(jobFunction, args, jobNum, jobTimeOutSec);
            this.jobsToRun.push(job);    
            this.jobLength ++ ;
        } catch (err) {
            console.error(err)
        } 
    }
    
    async _runParallel(job) {

        this._addToRunning(job);            
        job.setRunning();
        //console.log(`job start jobNum = [${job.jobNum}]`);  
        debugJobRunner(`job start jobNum = [${job.jobNum}]`);  
        try {
            const jobResult = await job.start();   
            clearTimeout(job.timer);       
            debugJobRunner(`job done jobNum = [${job.jobNum}]`); 
            this.emit('jobDone', jobResult, job); 
            job.setResolved();
            const resultToSave = {jobNum:job.jobNum, success:true};
            this.saveJobResults ? resultToSave.jobResult = jobResult : resultToSave.jobResult = undefined;
            this._addTotalResult(resultToSave);             
            this._delFromRunning(job.jobNum);

        } catch (err) {
            console.error(`job failed jobNum = [${job.jobNum}], err = [${err}]`);
            switch(err.code) {
                case 'TIMEOUT' :
                    this.emit('jobTimeOut', err, job);
                    break;
                default :
                    this.emit('jobError', err, job);                
            }
            job.setRejected();
            this._addTotalResult({jobNum:job.jobNum, success:false});   
            this._delFromRunning(job.jobNum);
            if(this.stopOnJobFailed) this.cancel();
        } finally {
            if(this.jobsToRun.length > 0 && !this.paused && !this.cancelled){
                const nextJob = this.jobsToRun.shift();
                this._runParallel(nextJob);
            }   
            this._isAllJobDone() ? this.lastCallback(this.totalResults) : this._notifyProgress(); 
        }
    }
    

    _notifyProgress() {
        const progress = `processed : ${this.totalResults.length}, success : ${this._getSuccessedJob().length}, failure : ${this._getFailedJob().length}`
        debugJobRunner(`job result : ${progress}`);
    }

    _getSuccessedJob(){
        return this.totalResults.filter((result) => {
            return result.success;
        })
    }

    _getFailedJob(){
        return this.totalResults.filter((result) => {
            return !result.success;
        })
    }

    _addToRunning(job){
        this.jobsRunning.push(job);
    }

    _getJobsRunning(){ 
        return this.jobsRunning;
    }

    _delFromRunning(jobNum){
        const index = this.jobsRunning.findIndex((job) => {
            return job.jobNum == jobNum;
        })
        delete this.jobsRunning.splice(index,1);
    }

    _addTotalResult(result){
        this.totalResults.push(result);
    }

    _isAllJobDone(){
        return (this.totalResults.length === this.jobLength);
    }

    start(concurrency){
        while(this.jobsToRun.length > 0 && !this.cancelled && this.jobsRunning.length < concurrency){
            const job = this.jobsToRun.shift();
            this._runParallel(job);
        }          
    }

    pause(){
        this.paused = true;
    }

    cancel(){
        this.cancelled = true;
    }

    resume(concurrency){
        this.paused = false;
        this.jobsToRun = [...this.jobsRunning,...this.jobsToRun];
        this.jobsRunning = [];
        while(this.jobsToRun.length > 0 && this.jobsRunning.length < concurrency){
            const job = this.jobsToRun.shift();
            this._runParallel(job);
        }
    }
}

module.exports = ParallelJobQueue;