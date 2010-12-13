\name{downloadS3File}
\alias{downloadS3File}
\title{AWS Support Function: Uploads a local file to an S3 Bucket...}
\usage{downloadS3File(bucketName, keyName, localFile)}
\description{AWS Support Function: Uploads a local file to an S3 Bucket}
\details{If buckName does not exist, it is created and a warning is issued.}
\author{James "JD" Long}
\arguments{\item{bucketName}{destination bucket}
\item{keyName}{key to download}
\item{localFile}{local file to be uploaded}}
