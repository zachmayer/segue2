

#' Converts an R list into a text CSV.
#' Serializes each element of an R list into ASCII characters then encodes then
#' for use as the input to a Hadoop Streaming job.
#' 

#' 
#' @param inList
#' @param outFileName
#' @return creates a CSV to file but returns nothing. 
#' @author James "JD" Long
#' @seealso csvToList
#' 
#' 
#' 
listToCsv <-
function(inList, outFileName){
  #require(caTools)
  if (is.list(inList) == FALSE) 
        stop("listToCsv: The input list fails the is.list() check.")
  fileName <- outFileName
  cat("", file=fileName, append=FALSE)
  
  i <- 1
  for (item in inList) {
    myLine <- paste(i, ",", base64encode(serialize(item, NULL, ascii=TRUE)), "\n", sep="")
    cat(myLine, file=fileName, append=TRUE) 
    i <- i+1
  }
}

