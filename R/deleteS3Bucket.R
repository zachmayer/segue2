deleteS3Bucket <-
function(bucketName){
  system(paste("s3cmd del --force s3://", bucketName,  "/*", sep=""))
  system(paste("s3cmd rb s3://", bucketName,  "/", sep=""))
}

