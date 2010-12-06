\name{csvToList}
\alias{csvToList}

\title{
The inverse of the listToCsv() function
}
\description{
Takes a csv of serialized objects created by the listToCSV function and
turns it into a proper R list. 
}
\usage{
csvToList(inFileName)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{inFileName}{
String pointing to the full path of the input CSV.
}
}
\value{
Returns a list object. Or an error. Hopefully a list. 
}

\author{
James "JD" Long
}

\seealso{
  listToCsv()
}
\examples{
  myList <- NULL
  set.seed(1)
  for (i in 1:10){
    a <- c(rnorm(999), NA)
    myList[[i]] <- a
  }
  listToCsv(myList, "/tmp/tst.csv")
  all.equal(myList,  csvToList("/tmp/tst.csv" ))

}
