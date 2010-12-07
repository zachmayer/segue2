

#' Parallel lapply() function using Amazon's EMR service.
#'
#' Parallel lapply() function for applying a function to every item in a list
#' using Amazon's EMR service.
#' 
#' 
#' @param X list to which the function will be applied
#' @param FUN function to apply
#' @param clusterObject cluster on which to run the process
#' @param \dots other params to pass to FUN
#' @return Output as a list
#' 
#' @export
emrlapply <-
function(X, FUN, clusterObject, ... ) {
 
  #set up a local temp directory
  myTempDir <- clusterObject$localTempDir

  #the function to apply gets put into myFunction
  myFun <- FUN
  funArgs <-  as.list(substitute(list(...)))[-1L]

  cranPackages <- clusterObject$cranPackages 
  
  #save the objects
  objectsFileName <-paste(myTempDir ,"/emrData.RData", sep="") 
  save(cranPackages,
       myFun,
       funArgs,  
       file = objectsFileName,
       compress="xz")

  #delete the contents of the s3TempDir and s3TempDirOut
  
  s3TempDir <- clusterObject$s3TempDir
  system(paste("s3cmd del --force s3://", s3TempDir,  "/*", sep=""))
  system(paste("s3cmd mb  s3://", s3TempDir,  "/", sep=""))
  
  s3TempDirOut <- clusterObject$s3TempDirOut
  system(paste("s3cmd del --force s3://", s3TempDirOut,  "/*", sep=""))
  system(paste("s3cmd rb s3://", s3TempDirOut,  "/", sep=""))
  
  #upload the datafile to S3
  system(paste("s3cmd put ", objectsFileName , " s3://", s3TempDir, 
              "/emrData.RData" , sep=""))
  
  #upload the mapper to S3
  #needs to be altered for a package
  system(paste("s3cmd put ", system.file("mapper.R", package="emrlapply")," s3://",
               s3TempDir,  "/mapper.R" , sep=""))
  
  #serialize the X list to a temp file
  streamFile <- paste(myTempDir, "/stream.txt", sep="")
  listToCsv(X, streamFile)
  
  #now upload stream.txt to EMR
  system(paste("s3cmd put ", streamFile , " s3://", s3TempDir, 
               "/stream.txt" , sep=""))
  
  finalStatus <- submitJob(clusterObject) 
  myTempDirOut <- clusterObject$localTempDirOut
  
  if (finalStatus %in% c("COMPLETED", "WAITING")) {
    system(paste("mkdir ", myTempDirOut, sep="" ))
    system(paste("rm ", myTempDirOut, "/*", sep=""))
    system(paste("s3cmd get  s3://", s3TempDirOut, 
              "/* ", myTempDirOut, "/", sep=""))

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
}

