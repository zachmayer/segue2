

#' Create a cluster by starting an EMR job
#' Creates all the local infrastructure for a cluster. This creates temp
#' directories locally, defins the elements of the cluster object.
#' 
#' Also creates the buckets on S3 for the cluster and uploads the bootstrap
#' code if needed. This function does everything except actually spin up the
#' EMR cluster.
#' 
#' 
#' @param numInstances Number of desired nodes. This is limited by Amazon to 20
#'   by default.
#' @param bootStrapLatestR Boolean (T or F) on whether the bootstrap script
#'   should run that installs the latest version of R from CRAN on each node.
#'   This slows down the starting of the nodes considerably. However, the
#'   default R on EMR was the one passed to the Children of Isreal by Moses.
#' @param cranPackages Text vector of the names of any CRAN packages you desire
#'   loaded.  Yes I know you have the most beautiful little package you wrote
#'   yourself which you want to install. This code does not support that...
#'   yet.
#' @param enableDebugging Thows Amazon EMR the debugging bit so you can log
#'   into the Amazon EMR dashboard and see debugging information. This slows
#'   things down. And be patient. Sometimes it takes 10 miutes for the debug
#'   data to populate the dashboard. Don't gripe to me about debugging speed on
#'   AMZN.
#' @return an emrlapply() cluster object with appropriate fields
#'   populated. Keep in mind that this creates the cluster but does not start
#'   it.
#' @author James "JD" Long
#' @seealso startCluster(), terminateCluster()
#' @examples
#' \dontrun{
#' myCluster   <- createCluster(numInstances=2,
#' bootStrapLatestR=TRUE, cranPackages=c("Hmisc", "plyr"))
#' }
#' @export
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
  makeS3Bucket(s3TempDir)
  
  #upload the bootstrapper to S3 if needed
  #how do I do this in a package? Right now this is hard coded
  if (bootStrapLatestR==TRUE) {
    uploadS3File(s3TempDir, system.file("bootstrap.sh", package="emrlapply"))
  }
  clusterObject$bootStrapLatestR <- bootStrapLatestR
  
  # start cluster
  #jobFlowId <- startCluster(numInstances, s3TempDir, s3TempDirOut, bootstrapLatestR)
  jobFlowId <- startCluster(clusterObject)
  
  clusterObject$jobFlowId <- jobFlowId
  
  return(clusterObject)
}

