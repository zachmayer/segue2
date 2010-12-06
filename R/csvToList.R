csvToList <-
function(inFileName){
  #require(caTools)
  linesIn <- readLines(inFileName, n=-1)
  outList <- NULL
  
  i <- 1
  for (line in linesIn){
    outList[[i]] <- unserialize(base64decode(strsplit(linesIn[[i]], split=",")[[1]][[2]], "raw"))
    i <- i+1
  }
  return(outList)
}

