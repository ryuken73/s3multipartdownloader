# s3 Multipart Downloader
javascript 기반의 s3 multipart download class (node.js)

# 배경
- aws sdk에서 mutipart download api는 제공되지만 download api는 없음
- 다운로드 속도향상과 이어내리기 기능을 위해서는 multipart download가 필요

## 특징
- Part Size 선택 가능 ( 최소 5MB, 단, part size가 999개가 넘지않도록 자동조정 )
- 동시 요청수 선택 가능 ( 최대 10개 )
- Download progress 제공 ( part별, 전체)
- 이어내리기 기능 : Todo
- Part Down 실패 시 자동 retry : Todo
- MD5 check : Todo

## 구성
- s3MultipartDownloader.js : class 정의 파일
- lib/jobRunner.js : 동시 작업 제어 라이브러리
- lib/debugger.js : debugging definition
- examples/app.js : 샘플 다운로드 프로그램
- awsconfig.json : aws 접속정보 (사용환경에 맞게 작성필요)
- s3params.json : s3 관련 정보 (사용환경에 맞게 작성필요)

## 사용법

1. git clone
```bash
# Clone this repository
git clone https://github.com/ryuken73/s3multipartdownloader.git
```
2. install dependency
```bash
npm install
```

3. awsconfig.json 작성 
- AWS IAM에서 새로운 계정만들고, S3에 권한을 부여
- access key와 secret을 받아서 아래의 양식처럼 작성

```javascript
{
    "accessKeyId" : "",
    "secretAccessKey" : "",
    "region" : ""
}
```

4. s3params.json 작성
- 사용할 S3 Bucket이름을 기술
```javascript
{
    "apiVersion" : "2006-03-01",
    "Bucket" : ""
}
```

5. Bucket에 package.json이라는 이름의 파일 업로드

6. sample 프로그램 구동
```bash
node examples/app.js
```
# 이벤트
```js
//emit every 500ms
downloader.on('progress', (progressInfo) => {})

progressInfo = { 
  startTime: 2018-12-03T01:16:51.933Z,
  endTime: undefined,
  srcName: 'file4.exe',
  dstName: 'c:\\temp\\file4.exe',
  fsize: 113728512,
  s3Header:
   { }, // s3 headers
  loaded: 28082860, // total loaded 
  loadedMB: '26.8MB',
  percent: 24.692893194628272,
  percentString: '24.7%',
  partSize: 10485760,
  partCounts: 11,
  concurrency: 10,
  loadedParts: 0, // total number of loaded
  avgSpeed: '53.5', // MB/s
  lastLoaded: NaN, // previous loaded
  lastLoadedMB: 'NaNMB',
  lastSpeed: 'NaN',
  remainSec: '2', // estimated remain seconds
  status: 'downloading' }
```

```js
//emit when part download done
downloader.on('partDownloaded', (result) => {})

result = { 
  partNum: 9,
  size: 10485760,
  range: 'bytes=83886080-94371840',
  startTime: 2018-12-03T01:16:52.899Z,
  endTime: 2018-12-03T01:16:53.843Z 
}
//
```

## License

[CC0 1.0 (Public Domain)](LICENSE.md)