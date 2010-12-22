#! /usr/bin/env Rscript

trimWhiteSpace <- function(line) gsub("(^ +)|( +$)", "", line)

## files from filesOnNodes are uploaded to a tmp directory
## this copies the files to the current working directory
fileList <- list.files("/tmp/segue-upload/", full.names=TRUE)
file.copy(fileList, getwd(), overwrite = TRUE)

con <- file("stdin", open = "r")
#con <- file("./stream.txt", open = "r")

pid <- as.character(Sys.getpid())
libPath <- paste("/tmp/R", pid, "/", sep='')
## if you don't want to use the Iowa State mirror for CRAN, you should change this
options(repos=c(CRAN="http://streaming.stat.iastate.edu/CRAN/"))
dir.create(libPath)

load("./emrData.RData") #contains:
                           # myPackages - list of packages
                           # myFun - Function to apply
                           # funArgs - the arguments passed
install.packages("bitops", lib=libPath)
install.packages("caTools", lib=libPath)
library(bitops,  lib=libPath)
library(caTools,  lib=libPath)


for (myPackage in cranPackages){
  #if a package fails to install or load everything bombs so added try()
  try(library(myPackage,  lib=libPath, character.only = T))
}

while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
  key <-  as.numeric(trimWhiteSpace(strsplit(line, split=",")[[1]][[1]]))
  value <- unserialize(base64decode(strsplit(line, split=",")[[1]][[2]], "raw"))
  value <- list(value)
  value <- c(value, funArgs)
  result <- do.call(myFun, value)
  
  #serialize and encode the result
  sresult <- paste("<result>,", key, ",", base64encode(serialize(result, NULL, ascii=T)), "\n", sep="")
  cat(sresult, "|\n", sep = "")
}
close(con)
