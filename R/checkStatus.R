#' Check the status of an EMR Job
#' Sends a request to EMR and returns the status of a job. 
#' 
#' @param jobFlowId an EMR convention of a given job. This is an element of a cluster in emrlapply once the cluster has been started.
#' @return a string value of the status.
#' @author James "JD" Long
#' @seealso getFinalStatus()

checkStatus <- function(jobFlowId){

  
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

