getFinalStatus <-
function(jobFlowId){
  while (checkStatus(jobFlowId)$ExecutionStatusDetail$State %in% c("COMPLETED", "FAILED",
                                                                   "TERMINATED", "WAITING")  == F) {
    message(paste((checkStatus(jobFlowId)$ExecutionStatusDetail$State), " - ", Sys.time(), sep="" ))
    Sys.sleep(30)
  }

  return(checkStatus(jobFlowId)$ExecutionStatusDetail$State)
}

