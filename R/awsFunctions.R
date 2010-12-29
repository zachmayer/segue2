
## A little simplification would be the first step toward rational living, I think.
## Eleanor Roosevelt 


## Lower logging level: LogManager.getLogManager().getLogger("com.amazonaws.request").setLevel(Level.OFF);
## ref: https://forums.aws.amazon.com/thread.jspa?messageID=186655&#186655

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
  #       there's some risk this might loop forever if they don't own the bucket
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
##' Does nothing if the bucketName does not exist. If bucket contains Keys,
##' all keys are deleted.
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
##' Creates an S3 bucket. If the bucket already exists, no warning is returned.
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

##' AWS Support Function: Downloads a key from an S3 Bucket into a local file.
##'
##' Pulls a key (file) from a bucket into a localFile. If the keyName = ".all" then
##' all files from the bucket are pulled and localFile should be a
##' directory name. Ignores "sub directories" in buckets. 
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
  
##' Creates the configuration object, uploads needed files, and starts
##' a Segue Hadoop cluster on Elastic Map Reduce. 
##'
##' The the needed files are uploaded to S3 and the EMR nodes are started.
##' @param numInstances number of nodes (EC2 instances)
##' @param cranPackages vector of string names of CRAN packages to load on each cluster node
##' @param filesOnNodes vector of string names of full path of files to be loaded on each node.
##' Files will be loaded into the local
##' path (i.e. ./file) on each node. 
##' @param rObjectsOnNodes a named list of R objects which will be passed to the R
##' session on the worker nodes. Be sure the list has names. The list will be attached
##' on the remote nodes using attach(rObjectsOnNodes). If you list does not have names,
##' this will fail.
##' @param enableDebugging T/F whether EMR debugging should be enabled
##' @param instancesPerNode Number of R instances per node. Default of NULL uses AWS defaults.
##' @param masterInstanceType EC2 instance type for the master node
##' @param slaveInstanceType EC2 instance type for the slave nodes
##' @param location EC2 location name for the cluster
##' @param ec2KeyName EC2 Key used for logging into the main node. Use the user name 'hadoop'
##' @param copy.image T/F whether to copy the entire local environment to the nodes. If this feels
##' fast and loose... you're right! It's nuts. Use it with caution. Very handy when you really need it.
##' @return an emrlapply() cluster object with appropriate fields
##'   populated. Keep in mind that this creates the cluster and starts the cluster running.
##' @author James "JD" Long
##' @examples
##' \dontrun{
##' myCluster   <- createCluster(numInstances=2,
##' bootStrapLatestR=TRUE, cranPackages=c("Hmisc", "plyr"))
##' }
##' @export
createCluster <- function(numInstances=2,
                          cranPackages=NULL,
                          filesOnNodes=NULL,
                          rObjectsOnNodes=NULL, 
                          enableDebugging=FALSE,
                          instancesPerNode=NULL,
                          masterInstanceType="m1.small",
                          slaveInstanceType="m1.small",
                          location = "us-east-1a",
                          ec2KeyName=NULL,
                          copy.image=FALSE
                          ){
  ## this used to be an argument but not bootstrapping
  ## caused too many problems
  bootStrapLatestR=TRUE 

  clusterObject <- list(numInstances = numInstances,
                        cranPackages = cranPackages,
                        enableDebugging = enableDebugging,
                        bootStrapLatestR = bootStrapLatestR,
                        filesOnNodes = filesOnNodes,
                        rObjectsOnNodes = rObjectsOnNodes, 
                        enableDebugging = enableDebugging,
                        instancesPerNode = instancesPerNode,
                        masterInstanceType = masterInstanceType,
                        slaveInstanceType = slaveInstanceType,
                        location = location,
                        ec2KeyName = ec2KeyName ,
                        copy.image = copy.image
                        )
  
  localTempDir <- paste(tempdir(),
                        paste(sample(c(0:9, letters), 10, rep=T), collapse=""),
                        "-segue",
                        sep="")
  
  clusterObject$localTempDir <- localTempDir
  clusterObject$localTempDirOut <- paste(localTempDir, "/out", sep="")

  system(paste("mkdir", localTempDir))
  system(paste("mkdir", clusterObject$localTempDirOut))

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
    ##TODO: error checking in the uploadS3File function
    uploadS3File(s3TempDir, system.file("bootstrapLatestR.sh", package="segue") )
    uploadS3File(s3TempDir, system.file("update.R", package="segue") )
    
  }
  clusterObject$bootStrapLatestR <- bootStrapLatestR

  ## if copy.image is TRUE then save an image and  use the fileOnNodes
  ## feature to add the saved image to the nodes
  if (copy.image == TRUE) {
    imageFile <- paste( localTempDir, "/local-workspace-image.RData", sep="" )
    save.image( file=imageFile, compress="xz" )
    clusterObject$filesOnNodes = c(clusterObject$filesOnNodes, imageFile)
  }
  
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
##' 
##' After a cluster has been defined with createCluster() this function actually
##' starts the machines running. Currently exported, but soon will be internal only.
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

  #creates the bootstrap list
  bootStrapList <- new( java.util.ArrayList )
  
  if (bootStrapLatestR == TRUE) {
   scriptBootActionConfig <- new(com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig)
   scriptBootActionConfig$setPath(paste("s3://", s3TempDir, "/bootstrapLatestR.sh", sep=""))
  
   bootStrapConfig <- new( com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig)
     with( bootStrapConfig, setScriptBootstrapAction(scriptBootActionConfig))
     with( bootStrapConfig, setName("R-InstallLatest"))
 
   bootStrapList$add(bootStrapConfig)

   ## update packages
   scriptBootActionConfig <- new(com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig)
   scriptBootActionConfig$setPath(paste("s3://", s3TempDir, "/update.R", sep=""))
  
   bootStrapConfig <- new( com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig)
     with( bootStrapConfig, setScriptBootstrapAction(scriptBootActionConfig))
     with( bootStrapConfig, setName("R-UpdatePackages"))
 
   bootStrapList$add(bootStrapConfig)
   
  }

  if (is.null(clusterObject$filesOnNodes) == FALSE) { # putting files on each node

    ## build a batch file that includes each element of filesOnNodes
    ## then add the batch file as a boot strap action

    ## open the output file (bootStrapFiles.sh) in clusterObject$tempDir
    ## open an output file connection
    outfile <- file( paste( clusterObject$localTempDir, "/bootStrapFiles.sh", sep="" ), "w" )  
    cat("#!/bin/bash", "", file = outfile, sep = "\n")
    cat("mkdir /tmp/segue-upload/", "", file = outfile, sep = "\n")
     ## for each element in filesOnNodes add a hadoop -fs line
    for ( file in clusterObject$filesOnNodes ){
      remotePath <- paste( "/tmp/segue-upload/", tail(strsplit(file,"/")[[1]], 1), sep="" )
      fileName <- tail(strsplit(file,"/")[[1]], 1)
      s3Path <- paste( "s3://", clusterObject$s3TempDir, "/", fileName, sep="" )
      cat( paste( "hadoop fs -get ", s3Path, remotePath)
          , file = outfile, sep = "\n" )
      cat( "\n", file = outfile )
      
      # copy each file to S3
      uploadS3File( clusterObject$s3TempDir, file )
    }
    close( outfile )
     # copy bootStrapFiles.sh to clusterObject$s3TempDir
    uploadS3File( clusterObject$s3TempDir, paste( clusterObject$localTempDir, "/bootStrapFiles.sh", sep="" ) )

     # add a bootstrap action that runs bootStrapFiles.sh
   scriptBootActionConfig <- new(com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig)
   scriptBootActionConfig$setPath(paste("s3://", s3TempDir, "/bootStrapFiles.sh", sep=""))
  
   bootStrapConfig <- new( com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig)
     with( bootStrapConfig, setScriptBootstrapAction(scriptBootActionConfig))
     with( bootStrapConfig, setName("RBootStrapFiles"))
 
   bootStrapList$add(bootStrapConfig)
  }

  if (is.null(clusterObject$instancesPerNode) == FALSE) { #sersiously... test this
   scriptBootActionConfig <- new(com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig)
   scriptBootActionConfig$setPath("s3://elasticmapreduce/bootstrap-actions/configure-hadoop")

   argList <- new( java.util.ArrayList )
   argList$add( "-s" )
   argList$add( paste( "mapred.tasktracker.map.tasks.maximum=", clusterObject$instancesPerNode, sep="") )
   argList$add( "-s" )
   argList$add( paste( "mapred.tasktracker.reducer.tasks.maximum=", clusterObject$instancesPerNode, sep="") )
 
   scriptBootActionConfig$setArgs( argList )
                                  
   bootStrapConfig <- new( com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig)
     with( bootStrapConfig, setScriptBootstrapAction(scriptBootActionConfig))
     with( bootStrapConfig, setName("SetInstancePerNode"))
 
   bootStrapList$add(bootStrapConfig)    
  }
          
   ## this adds the bootstrap to the request
  
   request$setBootstrapActions(bootStrapList)
  
  if ( is.null( clusterObject$ec2KeyName ) != TRUE ) {
      conf$setEc2KeyName(clusterObject$ec2KeyName)
  }
  
  conf$setHadoopVersion("0.20")
  
  #debugging... set to my personal key
  #conf$setEc2KeyName("ec2ApiTools")

  conf$setInstanceCount(new(Integer, as.character(numInstances)))
  conf$setKeepJobFlowAliveWhenNoSteps(new(Boolean, TRUE))
  conf$setMasterInstanceType( clusterObject$masterInstanceType )

  conf$setPlacement(new(com.amazonaws.services.elasticmapreduce.model.PlacementType, clusterObject$location))
  conf$setSlaveInstanceType( clusterObject$slaveInstanceType )
  request$setInstances(conf)
  request$setLogUri(paste("s3://", s3TempDir, "/logs", sep=""))
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
  return(jobFlowId)
  
  ## TODO: need to catch situations where the cluster failed
}

##' Stops a running cluster
##'
##' Stops a running cluster and deletes temporary directories from EC2
##' 
##' @return nothing
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

  ## I have no idea why AWS needs sleep before
  ## I can delete the temp dirs, but these fail
  ## if I don't have the sleep
  Sys.sleep(15)
  try( deleteS3Bucket(clusterObject$s3TempDir), silent=TRUE )
  try( deleteS3Bucket(clusterObject$s3TempDirOut), silent=TRUE )

  ## something weird is going on... I have to do this twice or it
  ## does not fully delete the s3TempDir's subdirectory
  ## will need to give this some attention later
  Sys.sleep(15)
  try( deleteS3Bucket(clusterObject$s3TempDir), silent=TRUE )
  try( deleteS3Bucket(clusterObject$s3TempDirOut), silent=TRUE )
  
}

##' Submits a job to a running cluster
##' 
##' After a cluster has been started this function submits jobs to that cluster.
##' If a job is submitted with enableDebugging=TRUE, all jobs submitted to that
##' cluster will also have debugging enabled. To turn debugging off, the cluster
##' must be stopped and restarted.
##' 
##' 
##' @param clusterObject a cluster object to submit to
##' @param stopClusterOnComplete set to true if you want the cluster to be shut down
##' after job completes
##' @return Execution status of this job
##' 
##' @export
submitJob <- function(clusterObject, stopClusterOnComplete=FALSE){
  jobFlowId       <- clusterObject$jobFlowId
  s3TempDir       <- clusterObject$s3TempDir
  s3TempDirOut    <- clusterObject$s3TempDirOut
  enableDebugging <- clusterObject$enableDebugging
 
  try(deleteS3Bucket(s3TempDirOut), silent=TRUE)
  unlink(clusterObject$localTempDirOut, recursive = TRUE)
  dir.create(clusterObject$localTempDirOut)
  
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
  argList$add( paste("s3n://", s3TempDir, "/emrData.RData#emrData.RData", sep=""))
  argList$add( "-input" )
  argList$add( paste("s3n://", s3TempDir, "/stream.txt", sep="") )
  argList$add( "-output" )
  argList$add( paste("s3n://", s3TempDirOut, "/", sep="") )
  argList$add( "-mapper" )
  argList$add( paste("s3n://", s3TempDir, "/mapper.R", sep="" ))
  argList$add( "-reducer" )
  argList$add( "cat" )

  hadoopJarStep$setArgs(argList)

  stepName <- format(Sys.time(), "%Y-%m-%d_%H:%M:%OS5") 
  
  stepConfig <- new(com.amazonaws.services.elasticmapreduce.model.StepConfig, stepName, hadoopJarStep)
  stepConfig$setActionOnFailure("CANCEL_AND_WAIT")
  
  stepList <- new( java.util.ArrayList )
  stepList$add( stepConfig )
  request <- new( com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest, jobFlowId, stepList)

  try(deleteS3Bucket(clusterObject$s3TempDirOut), silent=TRUE)
  
  #submit to EMR happens here
  service$addJobFlowSteps(request)

  Sys.sleep(15)

  checkLastStepStatus(jobFlowId)

  Sys.sleep(15)

  currentStatus <- checkStatus(jobFlowId)
  while (currentStatus  %in% c("COMPLETED", "FAILED", "TERMINATED", "WAITING", "CANCELLED")  == FALSE) {
    Sys.sleep(30)
    currentStatus <- checkStatus(jobFlowId)
    message(paste(currentStatus, " - ", Sys.time(), sep="" ))
  }
  if (stopClusterOnComplete==TRUE) {
    stopCluster(clusterObject)
  }
  return(currentStatus)
  
}

