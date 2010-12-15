# Lower logging level: LogManager.getLogManager().getLogger("com.amazonaws.request").setLevel(Level.OFF);
# ref: https://forums.aws.amazon.com/thread.jspa?messageID=186655&#186655

##' ##' AWS Support Function: set up credentials
##'
##' sets up the credentials needed to access AWS and optionally sets environment
##' variables for auto loading of credentials in the future
##' @param awsAccessKeyText your AWS Access Key as a string
##' @param awsSecretKeyText your AWS Secret Key as a string
##' @param setEnvironmentVariables T/F would you like environment variables to be set so
##' Segue will read the credentials on load
##' @author James "JD" Long
##' @export
setCredentials <- function(awsAccessKeyText, awsSecretKeyText, setEnvironmentVariables = TRUE){
    awsCreds <- new(com.amazonaws.auth.BasicAWSCredentials, awsAccessKeyText, awsSecretKeyText)
    assign("awsCreds", awsCreds, envir = .GlobalEnv)

    if (setEnvironmentVariables == TRUE) {
      Sys.setenv(AWSACCESSKEY = awsAccessKeyText, AWSSECRETKEY = awsSecretKeyText)
    }
}
##' AWS Support Function: Delete an S3 Key (a.k.a file)
##'
##' Deteles a key in a given bucket on S3
##' @param bucketName name of the bucket
##' @param keyName the key in the bucket
##' @author James "JD" Long
##' @export
deleteS3Key <- function(bucketName, keyName){
  tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
  s3 <- tx$getAmazonS3Client()
  s3$deleteObject(bucketName, keyName)
}

##' AWS Support Function: Empty an S3 bucket
##'
##' Deletes all keys in the designated bucket
##' @param bucketName Name of the bucket to be emptied
##' @author James "JD" Long
##' @export
emptyS3Bucket <- function(bucketName){
  tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
  s3 <- tx$getAmazonS3Client()
  
  lst <- s3$listObjects(bucketName)
  objSums <- lst$getObjectSummaries()
  listJavaObjs <- .jevalArray(objSums$toArray())

  for (i in 1:length(listJavaObjs)) {
    deleteS3Key(bucketName, listJavaObjs[[i]]$getKey()[[1]])
    #print(listJavaObjs[[i]]$getKey()[[1]])
  }
  if (lst$isTruncated()){
    #recursion FTW!
    emptyS3Bucket(bucketName)
  }
}


##' AWS Support Function: Delete an S3 Bucket
##'
##' Returns a warning if bucketName does not exist. If bucket contains Keys, all keys are deleted.
##' @param bucketName the bucket to be deleted
##' @author James "JD" Long
##' @export
deleteS3Bucket <- function(bucketName){
  emptyS3Bucket(bucketName)
  tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
  s3 <- tx$getAmazonS3Client()
  s3$deleteBucket(bucketName)
}

##' AWS Support Function: Creates an S3 Bucket
##'
##' Returns a warning if bucketName already exists.
##' @param bucketName string of the name of the bucket to be created
##' @author James "JD" Long
##' @export
makeS3Bucket <- function(bucketName){
    #awsCreds <- get("awsCreds", envir = segue.env)
    tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
    s3 <- tx$getAmazonS3Client()
    #test if the bucket exists; if not,  make bucket
    if (s3$doesBucketExist(bucketName) == FALSE) {
      s3$createBucket(bucketName)
    } else {
      warning("Unable to Create Bucket", call. = FALSE)
    }
}

##' AWS Support Function: Uploads a local file to an S3 Bucket
##'
##' If buckName does not exist, it is created and a warning is issued. 
##' @param bucketName destination bucket
##' @param localFile local file to be uploaded
##' @author James "JD" Long
##' @export
uploadS3File <- function(bucketName, localFile){
    tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
    s3 <- tx$getAmazonS3Client()
    fileToUpload <-  new(File, localFile)
    request <- new(com.amazonaws.services.s3.model.PutObjectRequest, bucketName, fileToUpload$getName(), fileToUpload)
    s3$putObject(request)
}

##' AWS Support Function: Uploads a local file to an S3 Bucket
##'
##' If buckName does not exist, it is created and a warning is issued. 
##' @param bucketName destination bucket
##' @param keyName key to download
##' @param localFile local file to be uploaded
##' @author James "JD" Long
##' @export
downloadS3File <- function(bucketName, keyName, localFile){
    tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
    s3 <- tx$getAmazonS3Client()

    request <- new(com.amazonaws.services.s3.model.GetObjectRequest, bucketName, keyName)
    theObject <- s3$getObject(request, new(java.io.File, localFile))
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
  
  localTempDir <- paste(tempdir(), paste(sample(c(0:9, letters), 10, rep=T), collapse=""), sep="")
  clusterObject$localTempDir <- localTempDir
  clusterObject$localTempDirOut <- paste(localTempDir, "/out", sep="")
  
  s3TempDir <- tolower(unlist(strsplit(localTempDir, "/"))[length(unlist(strsplit(localTempDir, "/")))])
  deleteS3Bucket(s3TempDir)
  clusterObject$s3TempDir <- s3TempDir
  
  s3TempDirOut <- tolower(paste(s3TempDir , "out", sep=""))
  deleteS3Bucket(s3TempDirOut)
  clusterObject$s3TempDirOut <- s3TempDirOut

  #create the s3 bucket
  makeS3Bucket(s3TempDir)
  
  #upload the bootstrapper to S3 if needed
  if (bootStrapLatestR==TRUE) {
    uploadS3File(system.file("bootstrap.sh", package="segue"), s3TempDir )
  }
  clusterObject$bootStrapLatestR <- bootStrapLatestR
  
  # start cluster
  jobFlowId <- startCluster(clusterObject)
  
  clusterObject$jobFlowId <- jobFlowId
  
  return(clusterObject)
}

##' AWS Support Function: Checks the status of a given job on EMR
##'
##' Checks the status of a previously issued job.
##' @param jobFlowId the Job Flow Id of the job to check
##' @return Job Status 
##' @author James "JD" Long
##' @export
checkStatus <- function(jobFlowId){


 jfDetails <- new( com.amazonaws.services.elasticmapreduce.model.JobFlowDetail )

 result <- new( com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsResult)
 result$setJobFlows()




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


##' Starts a cluster on Amazon's EMR service
##' After a cluster has been defined with createCluster() this function actually
##' starts the machines running.
##' 
##' @param clusterObject cluster object to start
##' @return a Job Flow ID
##' 
##' @export
startCluster <- function(clusterObject){
  numInstances     <- clusterObject$numInstances
  s3TempDir        <- clusterObject$s3TempDir 
  s3TempDirOut     <- clusterObject$s3TempDirOut
  bootStrapLatestR <- clusterObject$bootStrapLatestR
  verbose          <- TRUE
  numInstances     <- clusterObject$numInstances

 
  #### testing only ###
  s3TempDir <- "abc123test"
  ###  testing only ###

  service <- new( com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient, awsCreds )
  request <- new( com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest )
  conf    <- new( com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig )

  scriptBootActionConfig <- new(com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig)
  scriptBootActionConfig$setPath(paste("s3://", s3TempDir, "/bootstrap.sh", sep=""))

  bootStrapConfig <- new( com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig)
    with( bootStrapConfig, setScriptBootstrapAction(scriptBootActionConfig))
    with( bootStrapConfig, setName("RBootStrap"))
  
  bootStrapList <- new( java.util.ArrayList )
  bootStrapList$add(bootStrapConfig)
  request$setBootstrapActions(bootStrapList)
 
  ## TODO make keyname an argument
  #conf$setEc2KeyName(myKeyName);
  conf$setInstanceCount(new(Integer, "2"))
  conf$setKeepJobFlowAliveWhenNoSteps(new(Boolean, TRUE))
  conf$setMasterInstanceType("m1.small")

  conf$setPlacement(new(com.amazonaws.services.elasticmapreduce.model.PlacementType, "us-east-1a"))
  conf$setSlaveInstanceType("m1.small")
  request$setInstances(conf)
  request$setLogUri(paste("s3://", s3TempDir, "/logs ", sep=""))
  jobFlowName <- paste("RJob-", date(), sep="")
  request$setName(jobFlowName)

  result <- service$runJobFlow(request)
  jobFlowId <- result$getJobFlowId()
  
  checkStatus(jobFlowId)
     # loop for some period of time
     # wait for status to change to "waiting"
     # if status changes then say "running" otherwise throw an error

 ############ Original non-java API code  
 # fire up a cluster
 # returns NA if job fails
 #  emrCall <- paste("~/EMR/elastic-mapreduce --create --stream --name emrFromR ",
 #                   "--alive ", 
 #                   "--num-instances ", numInstances, " ", 
 #                   if (bootStrapLatestR==TRUE) {paste("--bootstrap-action  s3://",
 #                         s3TempDir, "/bootstrap.sh ", sep="")}, 
 #                   sep="")
 # 
 # emrCallReturn <- system(emrCall, intern=TRUE)
 # message(emrCallReturn)
 # if (substr(emrCallReturn, 1, 16)!= "Created job flow"){
 #   message(paste("The cluster did not launch properly. The command line was ", emrCall, sep=""))
 #   return(NA)
 #   stop()
 # } 
 #  jobFlowId <- substr(emrCallReturn, 18, nchar(emrCallReturn))
 #
 # while (checkStatus(jobFlowId)$ExecutionStatusDetail$State %in%
 #        c("COMPLETED", "FAILED", "TERMINATED", "WAITING", "CANCELLED")  == FALSE) {
 #   message(paste((checkStatus(jobFlowId)$ExecutionStatusDetail$State), " - ", Sys.time(), sep="" ))
 #   Sys.sleep(45)
 # }
 #
 # if (checkStatus(jobFlowId)$ExecutionStatusDetail$State == "WAITING") {
 #   message("Your Amazon EMR Hadoop Cluster is ready for action. \nRemember to terminate your cluster with terminateCluster().\nAmazon is billing you!")
 # }
 # 
 # if (checkStatus(jobFlowId)$ExecutionStatusDetail$State %in%
 #        c("COMPLETED", "WAITING")  == TRUE) {return(jobFlowId)}
 ############## End non-java api code

}

##' Submits a job to a running cluster
##' Submits a job to a running cluster
##' 
##' 
##' @param clusterObject a cluster object to submit to
##' @return Execution status of this job
##' 
##' @export
submitJob <- function(clusterObject){
  jobFlowId       <- clusterObject$jobFlowId
  s3TempDir       <- clusterObject$s3TempDir
  s3TempDirOut    <- clusterObject$s3TempDirOut
  enableDebugging <- clusterObject$enableDebugging

  deleteS3Bucket(s3TempDirOut)
  
  emrCall <- paste("~/EMR/elastic-mapreduce  --stream ",
                     " --jobflow ", jobFlowId, 
                     " --input s3n://", s3TempDir, "/stream.txt",
                     " --mapper s3n://", s3TempDir, "/mapper.R ",
                     " --reducer cat ",
                     " --output s3n://", s3TempDirOut, "/  ", 
                     " --cache s3n://", s3TempDir, "/emrData.RData#emrData.RData",
                     if (enableDebugging==TRUE) {" --enable-debugging "} ,
                   sep="")
  
  emrCallReturn <- system(emrCall, intern=TRUE)
  message(emrCallReturn)
  if (substr(emrCallReturn, 1, 14)!= "Added steps to"){
    message(paste("The job did not submit properly. The command line was ", emrCall, sep=""))
    return("Job Flow Creation Failed")
    stop()
  }
  Sys.sleep(15)
  if (enableDebugging==TRUE){Sys.sleep(45)} #debugging has to be set up on each job so it takes a bit
  
  while (checkStatus(jobFlowId)$ExecutionStatusDetail$State %in%
         c("COMPLETED", "FAILED", "TERMINATED", "WAITING", "CANCELLED")  == FALSE) {
    message(paste((checkStatus(jobFlowId)$ExecutionStatusDetail$State), " - ", Sys.time(), sep="" ))
    Sys.sleep(10)
  }
  return(checkStatus(jobFlowId)$ExecutionStatusDetail$State)
}

