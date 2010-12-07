\name{checkStatus}
\alias{checkStatus}
\title{Check the status of an EMR Job...}
\usage{checkStatus(jobFlowId)}
\description{Check the status of an EMR Job
Sends a request to EMR and returns the status of a job.}
\value{a string value of the status.}
\author{James "JD" Long}
\seealso{getFinalStatus()}
\arguments{\item{jobFlowId}{The jobFlowId is an EMR convention of a given job. This is
an element of a cluster in emrlapply once the cluster has been started.}}
