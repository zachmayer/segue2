listToCsv <-
function(inList, outFileName){
  #require(caTools)
  if (is.list(inList) == F) 
        stop("listToCsv: The input list fails the is.list() check.")
  fileName <- outFileName
  cat("", file=fileName, append=F)
  
  i <- 1
  for (item in inList) {
    myLine <- paste(i, ",", base64encode(serialize(item, NULL, ascii=T)), "\n", sep="")
    cat(myLine, file=fileName, append=T) 
    i <- i+1
  }
}

