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
  if (s3$doesBucketExist(bucketName)) { 
    s3$deleteObject(bucketName, keyName)
  }
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

  # TODO: need a check to make sure the current user owns the bucket
  #       before trying to delete everything in it
  if (s3$doesBucketExist(bucketName)) {  
    lst <- s3$listObjects(bucketName)
    objSums <- lst$getObjectSummaries()
    listJavaObjs <- .jevalArray(objSums$toArray())
    if (length(listJavaObjs)>0){
      for (i in 1:length(listJavaObjs)) {
        deleteS3Key(bucketName, listJavaObjs[[i]]$getKey()[[1]])
      }
    }
    if (lst$isTruncated()){
      #recursion FTW!
      emptyS3Bucket(bucketName)
    }
  }
}


##' AWS Support Function: Delete an S3 Bucket
##'
##' Does nothing if the bucketName does not exist. If bucket contains Keys, all keys are deleted.
##' @param bucketName the bucket to be deleted
##' @author James "JD" Long
##' @export
deleteS3Bucket <- function(bucketName){
  tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
  s3 <- tx$getAmazonS3Client()
  if (s3$doesBucketExist(bucketName) == TRUE) {
    emptyS3Bucket(bucketName)
    tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
    s3 <- tx$getAmazonS3Client()
    s3$deleteBucket(bucketName)
  }
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
##' Pulls a key (file) from a bucket into a localFile. If the keyName = ".all" then
##' all files from the bucket are pulled and localFile should be a
##' directory name. Ignores "sub directories" in buckets
##' @param bucketName destination bucket
##' @param keyName key to download. ".all" to pull all keys
##' @param localFile local file name or path if ".all" is called for keyName
##' @author James "JD" Long
##' @export
downloadS3File <- function(bucketName, keyName, localFile){
    tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
    s3 <- tx$getAmazonS3Client()
    if (keyName != ".all") {
      request <- new(com.amazonaws.services.s3.model.GetObjectRequest, bucketName, keyName)
      theObject <- s3$getObject(request, new(java.io.File, localFile))
    } else {
     # this will only pull the first page of listings
     # so if there are a lot of files it won't grab them all
     # 
     # TODO: make it pull multiple pages of files
     # TODO: pull subdirectories too
      system(paste("mkdir", localFile), ignore.stderr = TRUE)
      lst <- s3$listObjects(bucketName)
      objSums <- lst$getObjectSummaries()
      listJavaObjs <- .jevalArray(objSums$toArray())
      if (length(listJavaObjs)>0){
        for (i in 1:length(listJavaObjs)) {
          # if statement here just to filter out subdirs
          key <- listJavaObjs[[i]]$getKey()[[1]]
          if ( length( unlist(strsplit(key, split="/")) ) == 1) {
            if (substring( key, nchar( key ) - 7, nchar( key ) )  != "$folder$") {
              localFullFile <- paste(localFile, "/", listJavaObjs[[i]]$getKey()[[1]], sep="")
              downloadS3File(bucketName, listJavaObjs[[i]]$getKey()[[1]], localFullFile)
            }
          }
        }
      }
    }
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
  ## TODO: error check this
  makeS3Bucket(s3TempDir)
  
  #upload the bootstrapper to S3 if needed
  if (bootStrapLatestR==TRUE) {
    ##TODO: error checkign in the uploadS3File function
    uploadS3File(s3TempDir, system.file("bootstrap.sh", package="segue") )
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
  service <- new( com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient, awsCreds )
  request <- new( com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsRequest )
  detailsList <- new( java.util.ArrayList )
  detailsList$add(jobFlowId)
  request$setJobFlowIds(detailsList)
  descriptions <- as.list(service$describeJobFlows(request)$getJobFlows())
  descriptions[[1]]$getExecutionStatusDetail()$getState()
}

##' AWS Support Function: Checks the status of a given job on EMR
##'
##' Checks the status of a previously issued step.
##' @param jobFlowId the Job Flow Id of the job to check
##' @return Status of the last step 
##' @author James "JD" Long
##' @export
checkLastStepStatus <- function(jobFlowId){
  service <- new( com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient, awsCreds )
  request <- new( com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsRequest )
  detailsList <- new( java.util.ArrayList )
  detailsList$add(jobFlowId)
  request$setJobFlowIds(detailsList)
  descriptions <- as.list(service$describeJobFlows(request)$getJobFlows())
  #descriptions[[1]]$getExecutionStatusDetail()$getState()

  steps <- as.list(descriptions[[1]]$getSteps())
  step <- steps[[length(steps)]] #grab the last step only
  status <- step$getExecutionStatusDetail()
  status$getState()
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
 
  service <- new( com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient, awsCreds )
  request <- new( com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest )
  conf    <- new( com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig )

  if (bootStrapLatestR == TRUE) {
   scriptBootActionConfig <- new(com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig)
   scriptBootActionConfig$setPath(paste("s3://", s3TempDir, "/bootstrap.sh", sep=""))
  
   bootStrapConfig <- new( com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig)
     with( bootStrapConfig, setScriptBootstrapAction(scriptBootActionConfig))
     with( bootStrapConfig, setName("RBootStrap"))
  
   bootStrapList <- new( java.util.ArrayList )
   bootStrapList$add(bootStrapConfig)
   request$setBootstrapActions(bootStrapList)
  }
  
  ## TODO add the following arguments:
     # placement location
     # master instance type
     # slave instance type
     # key name
  
  #conf$setEc2KeyName(myKeyName);
  conf$setInstanceCount(new(Integer, as.character(numInstances)))
  conf$setKeepJobFlowAliveWhenNoSteps(new(Boolean, TRUE))
  conf$setMasterInstanceType("m1.small")

  conf$setPlacement(new(com.amazonaws.services.elasticmapreduce.model.PlacementType, "us-east-1a"))
  conf$setSlaveInstanceType("m1.small")
  request$setInstances(conf)
  request$setLogUri(paste("s3://", s3TempDir, "/logs ", sep=""))
  jobFlowName <- paste("RJob-", date(), sep="")
  request$setName(jobFlowName)

  result <- service$runJobFlow(request)

  ## seems like this sleep should not be needed... but otherwise
  ## getJobFlowId() does not get the correct jobflowid

  Sys.sleep(15)
  jobFlowId <- result$getJobFlowId()

  currentStatus <- checkStatus(jobFlowId)
  while (currentStatus  %in% c("COMPLETED", "FAILED", "TERMINATED", "WAITING", "CANCELLED")  == FALSE) {
    Sys.sleep(30)
    currentStatus <- checkStatus(jobFlowId)
    message(paste(currentStatus, " - ", Sys.time(), sep="" ))
  }
 
  if (currentStatus == "WAITING") {
    message("Your Amazon EMR Hadoop Cluster is ready for action. \nRemember to terminate your cluster with stopCluster().\nAmazon is billing you!")
  }
  
  if (currentStatus %in% c("COMPLETED", "WAITING")  == TRUE) {
    return(jobFlowId)
  }
  ## TODO: need to catch situations where the cluster failed

}


##' Stops a running cluster
##'
##' Stops a running cluster - known as clusterFuck() and terminateCluster() in previous versions
##' 
##' @return not really sure - Jack Shit, I think
##' @author James "JD" Long
##' @param clusterObject a cluster object to stop
##' @export
stopCluster <- function(clusterObject){
  jobFlowId <- clusterObject$jobFlowId

  service <- new( com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient, awsCreds )
  request <- new( com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest )
  detailsList <- new( java.util.ArrayList )
  detailsList$add(jobFlowId)
  request$withJobFlowIds(detailsList)
  service$terminateJobFlows(request)
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

  try(deleteS3Bucket(s3TempDirOut), silent=TRUE)

  jobFlowId <- clusterObject$jobFlowId

  if (enableDebugging==TRUE){
    service <- new( com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient, awsCreds )
    hadoopJarStep <- new(com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig)
    hadoopJarStep$setJar("s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar")
    argList <- new( java.util.ArrayList )
    argList$add( "s3://us-east-1.elasticmapreduce/libs/state-pusher/0.1/fetch" )
    hadoopJarStep$setArgs(argList)
    stepName <- format(Sys.time(), "%Y-%m-%d_%H:%M:%OS5") 
    stepConfig <- new(com.amazonaws.services.elasticmapreduce.model.StepConfig, stepName, hadoopJarStep)
    stepConfig$setActionOnFailure("CANCEL_AND_WAIT")
    stepList <- new( java.util.ArrayList )
    stepList$add( stepConfig )
    request <- new( com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest, jobFlowId, stepList)
    requestResult <- service$addJobFlowSteps(request)
  }
  
  service <- new( com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient, awsCreds )

  hadoopJarStep <- new(com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig)
  hadoopJarStep$setJar("/home/hadoop/contrib/streaming/hadoop-streaming.jar")
  argList <- new( java.util.ArrayList )
  argList$add( "-cacheFile" )
  argList$add( paste("s3://", s3TempDir, "/emrData.RData#emrData.RData", sep=""))
  argList$add( "-input" )
  argList$add( paste("s3://", s3TempDir, "/stream.txt", sep="") )
  argList$add( "-output" )
  argList$add( paste("s3://", s3TempDirOut, "/", sep="") )
  argList$add( "-mapper" )
  argList$add( paste("s3://", s3TempDir, "/mapper.R", sep="" ))
  argList$add( "-reducer" )
  argList$add( "cat" )
             
  #if (enableDebugging==TRUE) {argList$add( "-enable-debugging" )}
  hadoopJarStep$setArgs(argList)

  stepName <- format(Sys.time(), "%Y-%m-%d_%H:%M:%OS5") 
  
  stepConfig <- new(com.amazonaws.services.elasticmapreduce.model.StepConfig, stepName, hadoopJarStep)
  stepConfig$setActionOnFailure("CANCEL_AND_WAIT")
  
  stepList <- new( java.util.ArrayList )
  stepList$add( stepConfig )
  request <- new( com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest, jobFlowId, stepList)

  try(deleteS3Bucket(clusterObject$s3TempDirOut), silent=TRUE)
  
  #start step
  service$addJobFlowSteps(request)

  Sys.sleep(15)

  checkLastStepStatus(jobFlowId)

  Sys.sleep(15)

  if (enableDebugging==TRUE){Sys.sleep(30)} #debugging has to be set up on each job so it takes a bit

  currentStatus <- checkStatus(jobFlowId)
  while (currentStatus  %in% c("COMPLETED", "FAILED", "TERMINATED", "WAITING", "CANCELLED")  == FALSE) {
    Sys.sleep(30)
    currentStatus <- checkStatus(jobFlowId)
    message(paste(currentStatus, " - ", Sys.time(), sep="" ))
  }
  return(currentStatus)
}

