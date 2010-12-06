
initializeAwsSdk <- function(){
    library(rJava)
    .jinit()

    awsAccessKeyText <- Sys.getenv("AWSACCESSKEY") ## have to have the keys set up in the .Renviron
    awsSecretKeyText <- Sys.getenv("AWSSECRETKEY")
    pathToSdk <- "/home/jal/aws-java-sdk-1.1.0/"

    .jaddClassPath(paste(pathToSdk, "lib/aws-java-sdk-1.1.0.jar", sep=""))
    .jaddClassPath(paste(pathToSdk, "third-party/commons-logging-1.1.1/commons-logging-1.1.1.jar", sep=""))
    .jaddClassPath(paste(pathToSdk, "third-party/commons-httpclient-3.0.1/commons-httpclient-3.0.1.jar", sep=""))
    .jaddClassPath(paste(pathToSdk, "third-party/commons-codec-1.3/commons-codec-1.3.jar", sep=""))

    attach( javaImport( "java.lang" ) )
    attach( javaImport( "java.io" ) )
}

