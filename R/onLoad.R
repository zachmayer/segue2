.onLoad <- function(libname, pkgname) {

  .jpackage(pkgname, lib.loc = libname)

  if (Sys.getenv("AWSACCESSKEY") != "" && Sys.getenv("AWSSECRETKEY") != ""){
    awsCreds <- new(com.amazonaws.auth.BasicAWSCredentials, Sys.getenv("AWSACCESSKEY"), Sys.getenv("AWSSECRETKEY"))
    assign("awsCreds", awsCreds, envir = .GlobalEnv)
    packageStartupMessage( "Segue has loaded your AWS Credentials." )
  } else {
    packageStartupMessage( "Segue did not find your AWS credentials. Please run the setCredentials() function." )
  }
}