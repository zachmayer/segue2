

##' Parallel lapply() function using Amazon's EMR service.
##'
##' Parallel lapply() function for applying a function to every item in a list
##' using Amazon's EMR service.
##' 
##' 
##' @param X list to which the function will be applied
##' @param FUN function to apply
##' @param clusterObject cluster on which to run the process
##' @param \dots other params to pass to FUN
##' @return Output as a list
##' 
##' @export
emrlapply <- function(X, FUN, clusterObject, ... ) {
  #set up a local temp directory
  myTempDir <- clusterObject$localTempDir

  #the function to apply gets put into myFunction
  myFun <- FUN
  funArgs <-  as.list(substitute(list(...)))[-1L]

  ## TESTING!!!
  #funArgs <- convertArgs(na.rm=TRUE)
  
  cranPackages <- clusterObject$cranPackages 
  
  #save the objects
  objectsFileName <-paste(myTempDir ,"/emrData.RData", sep="") 
  save(cranPackages,
       myFun,
       funArgs,  
       file = objectsFileName,
       compress="xz")

  #make sure the bucket exists, and is empty
  try(makeS3Bucket(clusterObject$s3TempDir), silent=TRUE)
  emptyS3Bucket(clusterObject$s3TempDir)

  #the out director must NOT exist
  try(deleteS3Bucket(clusterObject$s3TempDirOut), silent=TRUE)
  
  #upload the datafile to S3
  uploadS3File(clusterObject$s3TempDir, paste(objectsFileName, sep=""))
    
  #upload the mapper to S3
  #needs to be altered for a package
  uploadS3File(clusterObject$s3TempDir, system.file("mapper.R", package="emrlapply"))

  #serialize the X list to a temp file
  streamFile <- paste(myTempDir, "/stream.txt", sep="")
  listToCsv(X, streamFile)
  
  #now upload stream.txt to EMR
  uploadS3File(clusterObject$s3TempDir, streamFile)
  
  finalStatus <- submitJob(clusterObject) 
  myTempDirOut <- clusterObject$localTempDirOut

  
  #if (finalStatus %in% c("COMPLETED", "WAITING")) {
  #  system(paste("mkdir ", myTempDirOut, sep="" ))
  #  system(paste("rm ", myTempDirOut, "/*", sep=""))

  downloadS3File(clusterObject$s3TempDirOut, ".all", myTempDirOut)
   
    #open files
  returnedFiles <- list.files(path=myTempDirOut, pattern="part")
    #yes, I read all the results into R then write them out to a text file
    #There was a reason for doing this, but I don't remember it
    #this could all be done in one step
  combinedOutputFile <- file(paste(myTempDirOut, "/combinedOutput.csv", sep=""), "w")
  unparsedOutput <- NULL
    for (file in returnedFiles){
        lines <- readLines(paste(myTempDirOut, "/", file, sep="")) 
        for (line in lines) {
          if (substr(line, 1, 9) == "<result>,") {
            write(substr(line, 10, nchar(line)), file=combinedOutputFile)
          }
        }
    }
    close(combinedOutputFile)
    
    #require(caTools)
    lines <- strsplit(readLines(paste(myTempDirOut, "/combinedOutput.csv", sep="")),
                      split=",")
    output <- NULL
    
    for (i in 1:length(lines)){
      output[[as.numeric(lines[[i]][[1]])]] <- (unserialize(base64decode(substr(lines[[i]][[2]],
                                                                               1, nchar(lines[[i]][[2]])-1),
                                                                        "raw")))
    }
    return(as.list(output))
}


