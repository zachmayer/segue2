#! /usr/bin/env Rscript

trimWhiteSpace <- function(line) gsub("(^ +)|( +$)", "", line)

## files from filesOnNodes are uploaded to a tmp directory
## this copies the files to the current working directory
try( fileList <- list.files("/tmp/segue-upload/", full.names=TRUE ), silent=TRUE )
try( file.copy(fileList, getwd(), overwrite = TRUE), silent=TRUE)

## try to load the saved workplace image file. This will silently
## fail if there is no workspace file to load
try( load(file="local-workspace-image.RData"), silent=TRUE )

con <- file("stdin", open = "r")

## the libPath is set to include the process ID so that
## multiple instances of R on the same node don't have a
## library locking conflict
pid <- as.character(Sys.getpid())
libPath <- paste("/tmp/R", pid, "/", sep='')

## if you don't want to use the main CRAN site, you should
## change this to a mirror
options(repos=c(CRAN="http://cran.r-project.org/"))
dir.create(libPath)

load("./emrData.RData") #contains:
                           # cranPackages - list of packages
                           # myFun - Function to apply
                           # funArgs - the arguments passed
                           # rObjectsOnNodes - a NAMED list of R objects the users wants
                           #                   on each node

attach(rObjectsOnNodes)

install.packages("bitops", lib=libPath)
install.packages("caTools", lib=libPath)
library(bitops,  lib=libPath)
library(caTools,  lib=libPath)


for (myPackage in cranPackages){
  try(install.packages(myPackage, lib=libPath) )
  try(library(myPackage,  lib=libPath, character.only = TRUE))
  cat("finished installing")
}

while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
  cat("started readlines \n")
  key <-  as.numeric(trimWhiteSpace(strsplit(line, split=",")[[1]][[1]]))
  value <- unserialize(base64decode(strsplit(line, split=",")[[1]][[2]], "raw"))
  value <- list(value)
  value <- c(value, funArgs)
  result <- do.call(myFun, value) # can you believe this one short line does
                                  # all the work?!? 
  
  #serialize and encode the result
  sresult <- paste("<result>,", key, ",", base64encode(serialize(result, NULL, ascii=T)), "\n", sep="")
  cat(sresult, "|\n", sep = "")
}
close(con)
