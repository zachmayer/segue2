

#' Submits a job to a running cluster
#' Submits a job to a running cluster
#' 

#' 
#' @param clusterObject cluster to submit to
#' @return Execution status of a job
#' 
#' 
submitJob <-
function(clusterObject){
  jobFlowId    <- clusterObject$jobFlowId
  s3TempDir    <- clusterObject$s3TempDir
  s3TempDirOut <- clusterObject$s3TempDirOut
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

