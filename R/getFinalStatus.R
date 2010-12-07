

##' Gets the final job status for an EMR job.
##' Gets the final job status for an EMR job. If the job is running it waits
##' until it hits a terminal status. This is a looping wrapper for
##' checkStatus().
##' 
##' 
##' @param jobFlowId The jobFlowId is an EMR convention of a given job. This is
##'   an element of a cluster in emrlapply once the cluster has been started.
##' @return Currently returns one of "COMPLETED", "FAILED", "TERMINATED", or
##'   "WAITING". This might change if EMR begins supporting terminal statuses
##'   (stati?) other than the ones above.
##' @author James "JD" Long
##' @seealso checkStatus()
##' @export
getFinalStatus <-
function(jobFlowId){
  while (checkStatus(jobFlowId)$ExecutionStatusDetail$State %in% c("COMPLETED", "FAILED",
                                                                   "TERMINATED", "WAITING")  == FALSE) {
    message(paste((checkStatus(jobFlowId)$ExecutionStatusDetail$State), " - ", Sys.time(), sep="" ))
    Sys.sleep(30)
  }

  return(checkStatus(jobFlowId)$ExecutionStatusDetail$State)
}

