
##' AWS Support Function: Delete an S3 Bucket
##'
##' Returns a warning if bucketName does not exist.
##' @param bucketName the bucket to be deleted
##' @author James Long
##' @export
deleteS3Bucket <- function(bucketName){
  system(paste("s3cmd del --force s3://", bucketName,  "/*", sep=""))
  system(paste("s3cmd rb s3://", bucketName,  "/", sep=""))
}
##' AWS Support Function: Creates an S3 Bucket
##'
##' Returns a warning if bucketName already exists.
##' @param bucketName the bucket to be created
##' @author James Long
##' @export
makeS3Bucket <- function(bucketName){
    #awsCreds <- get("awsCreds", envir = segue.env)
    tx       <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
    s3 <- tx$getAmazonS3Client()
    #test if the bucket exists; if not,  make bucket
    if (s3$doesBucketExist(bucketName) == FALSE) {
      s3$createBucket(bucketName)
    }
}
##' AWS Support Function: Uploads a local file to an S3 Bucket
##'
##' If buckName does not exist, it is created and a warning is issued. 
##' @param bucketName destination bucket
##' @param localFile local file to be uploaded
##' @author James Long
##' @export
uploadS3File <- function(bucketName, localFile){
    #awsCreds <- get("awsCreds", envir = segue.env)
    tx       <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
    s3 <- tx$getAmazonS3Client()
    fileToUpload <-  new(File, localFile)
    request <- new(com.amazonaws.services.s3.model.PutObjectRequest, bucketName, fileToUpload$getName(), fileToUpload)
    s3$putObject(request)
}


##' AWS Support Function: Creates a Hadoop cluster on Elastic Map Reduce.
##'
##' The the needed files are uploaded to S3 and the EMR nodes are started.
##' @param numInstances number of nodes (EC2 instances)
##' @param bootStrapLatestR T/F whether or not to load the latest R from CRAN
##' @param cranPackages vector of string names of CRAN packages to load on each cluster node
##' @param enableDebugging T/F whether EMR debugging should be enabled
##' @author James "JD" Long
##' @export
createCluster <- function(numInstances=2, bootStrapLatestR=TRUE,
                          cranPackages=NULL, enableDebugging=FALSE){
  #TODO: add support for different instance sizes

  clusterObject <- list(numInstances=numInstances,
                        cranPackages=cranPackages,
                        enableDebugging=enableDebugging,
                        bootStrapLatestR=bootStrapLatestR)
  
  localTempDir <- tempdir()
  clusterObject$localTempDir <- localTempDir
  clusterObject$localTempDirOut <- paste(localTempDir, "/out", sep="")
  
  s3TempDir <- tolower(unlist(strsplit(localTempDir, "/"))[length(unlist(strsplit(localTempDir, "/")))])
  deleteS3Bucket(s3TempDir)
  clusterObject$s3TempDir <- s3TempDir
  
  s3TempDirOut <- tolower(paste(s3TempDir , "out", sep=""))
  deleteS3Bucket(s3TempDirOut)
  clusterObject$s3TempDirOut <- s3TempDirOut

  #create the s3 bucket
  system(paste("s3cmd mb s3://",
               s3TempDir, sep=""))
  
  #upload the bootstrapper to S3 if needed
  #how do I do this in a package? Right now this is hard coded
  if (bootStrapLatestR==TRUE) {
    system(paste("s3cmd put ", system.file("bootstrap.sh", package="emrlapply")," s3://",
               s3TempDir,  "/bootstrap.sh" , sep=""))
  }
  clusterObject$bootStrapLatestR <- bootStrapLatestR
  
  # start cluster
  #jobFlowId <- startCluster(numInstances, s3TempDir, s3TempDirOut, bootstrapLatestR)
  jobFlowId <- startCluster(clusterObject)
  
  clusterObject$jobFlowId <- jobFlowId
  
  return(clusterObject)
}
##' AWS Support Function: Checks the status of a given job on EMR
##'
##' Checks the status of a previously issued job.
##' @param jobFlowId the Job Flow Id of the job to check
##' @return Job Status 
##' @author James Long
##' @export
checkStatus <- function(jobFlowId){
  # this works best if this change mentioned in this article is made
  # http://developer.amazonwebservices.com/connect/thread.jspa?threadID=46583&tstart=60
  # Otherwise I had issues with the request timing out
  
  #require(rjson)
  emrJson <- paste(system(paste("~/EMR/elastic-mapreduce --describe --jobflow ",
                                jobFlowId, sep=""), intern=TRUE))
  emrJson <- gsub("\\\\", "\\", emrJson) #handle the double escaped text
  parser <- newJSONParser()
    
  for (i in 1:length(emrJson)){
      parser$addData(emrJson[i])
  }
    
  return(parser$getObject()[[1]][[1]])
}
