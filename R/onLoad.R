.onLoad <- function(libname, pkgname) {

  .jpackage(pkgname, lib.loc = libname)

  #TODO: use get and set options
  if (Sys.getenv("AWSACCESSKEY") != "" && Sys.getenv("AWSSECRETKEY") != ""){
    awsCreds <- new(com.amazonaws.auth.BasicAWSCredentials, Sys.getenv("AWSACCESSKEY"), Sys.getenv("AWSSECRETKEY"))
    options(awsCreds = awsCreds)
  }
}