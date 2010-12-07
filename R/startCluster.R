startCluster <-
function(clusterObject){
  numInstances     <- clusterObject$numInstances
  s3TempDir        <- clusterObject$s3TempDir 
  s3TempDirOut     <- clusterObject$s3TempDirOut
  bootStrapLatestR <- clusterObject$bootStrapLatestR
  verbose          <- TRUE
  numInstances     <- clusterObject$numInstances

 # fire up a cluster
 # returns NA if job fails
  emrCall <- paste("~/EMR/elastic-mapreduce --create --stream --name emrFromR ",
                    "--alive ", 
                    "--num-instances ", numInstances, " ", 
                    if (bootStrapLatestR==TRUE) {paste("--bootstrap-action  s3://",
                          s3TempDir, "/bootstrap.sh ", sep="")}, 
                    sep="")
  
  emrCallReturn <- system(emrCall, intern=TRUE)
  message(emrCallReturn)
  if (substr(emrCallReturn, 1, 16)!= "Created job flow"){
    message(paste("The cluster did not launch properly. The command line was ", emrCall, sep=""))
    return(NA)
    stop()
  } 

  jobFlowId <- substr(emrCallReturn, 18, nchar(emrCallReturn))

  while (checkStatus(jobFlowId)$ExecutionStatusDetail$State %in%
         c("COMPLETED", "FAILED", "TERMINATED", "WAITING", "CANCELLED")  == FALSE) {
    message(paste((checkStatus(jobFlowId)$ExecutionStatusDetail$State), " - ", Sys.time(), sep="" ))
    Sys.sleep(45)
  }

  if (checkStatus(jobFlowId)$ExecutionStatusDetail$State == "WAITING") {
    message("Your Amazon EMR Hadoop Cluster is ready for action. \nRemember to terminate your cluster with terminateCluster().\nAmazon is billing you!")
  }
  
  if (checkStatus(jobFlowId)$ExecutionStatusDetail$State %in%
         c("COMPLETED", "WAITING")  == TRUE) {return(jobFlowId)}
}

