

#' The inverse of the listToCsv() function
#' Takes a csv of serialized objects created by the listToCSV function and
#' turns it into a proper R list.
#' 
#' 
#' @param inFileName String pointing to the full path of the input CSV.
#' @return Returns a list object. Or an error. Hopefully a list.
#' @author James "JD" Long
#' @seealso listToCsv()
#' @examples
#'   myList <- NULL
#'   set.seed(1)
#'   for (i in 1:10){
#'     a <- c(rnorm(999), NA)
#'     myList[[i]] <- a
#'   }
#' 
#'   require(caTools)
#'   listToCsv(myList, "tst.csv")
#'   all.equal(myList,  csvToList("tst.csv" ))
#' 
#' @export
csvToList <- function(inFileName){
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

