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

