\name{getFinalStatus}
\alias{getFinalStatus}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Gets the final job status for an EMR job.
}
\description{
Gets the final job status for an EMR job. If the job is running it waits
until it hits a terminal status. This is a looping wrapper for checkStatus().
}
\usage{
getFinalStatus(jobFlowId)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{jobFlowId}{
The jobFlowId is an EMR convention of a given job. This is an element of
a cluster in emrlapply once the cluster has been started.
}
}

\value{
Currently returns one of "COMPLETED", "FAILED", "TERMINATED", or
"WAITING". This might change if EMR begins supporting terminal statuses
(stati?) other than the ones above. 
}
\author{
James "JD" Long
}
\seealso{
checkStatus()
}
