terminateCluster <-
function(clusterObject, deleteTemp=T){
  system(paste("~/EMR/elastic-mapreduce --terminate --jobflow ", clusterObject$jobFlowId, sep=""), intern=T)
  if (deleteTemp==T) {
    deleteS3Bucket(clusterObject$s3TempDir)
    deleteS3Bucket(clusterObject$s3TempDirOut)
    unlink(clusterObject$localTempDir, recursive = T)
    unlink(clusterObject$localTempDirOut, recursive = T)
  }
}

