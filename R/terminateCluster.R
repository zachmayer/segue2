

#' Terminates a running clsuter.
#' Terminates a running clsuter.
#' 

#' 
#' @param clusterObject cluster to terminate
#' @param deleteTemp T/F whether to delete temp files 
#' @export
#' 
terminateCluster <-
function(clusterObject, deleteTemp=TRUE){
  system(paste("~/EMR/elastic-mapreduce --terminate --jobflow ", clusterObject$jobFlowId, sep=""), intern=TRUE)
  if (deleteTemp==TRUE) {
    deleteS3Bucket(clusterObject$s3TempDir)
    deleteS3Bucket(clusterObject$s3TempDirOut)
    unlink(clusterObject$localTempDir, recursive = TRUE)
    unlink(clusterObject$localTempDirOut, recursive = TRUE)
  }
}

