
### <SETUP STUFF> the following lines need to be put somewhere else to be run once 
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
### </SETUP STUFF>  ###########


awsCreds <- new(com.amazonaws.auth.BasicAWSCredentials, awsAccessKeyText, awsSecretKeyText)


tx       <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)

bucketName  <- paste("s3-upload-sdk-sample-", tolower(awsAccessKeyText), sep="")
s3 <- tx$getAmazonS3Client()

#test if the bucket exists; if not,  make bucket
if (s3$doesBucketExist(bucketName) == FALSE) {
  s3$createBucket(bucketName)
}

fileToUpload <-  new(File, new(String, testFile))

request <- new(com.amazonaws.services.s3.model.PutObjectRequest, bucketName, fileToUpload$getName(), fileToUpload)

s3$putObject(request)
