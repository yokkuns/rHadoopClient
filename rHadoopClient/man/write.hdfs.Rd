\name{write.hdfs}
\alias{write.hdfs}
\title{
Writing HDFS data
}
\description{
Writing HDFS data
}
\usage{
write.hdfs(data, hdfs.path, hadoop.cmd = "hadoop", sep = "\t")
}
\arguments{
  \item{data}{
    data.frame
}
  \item{hdfs.path}{
    hdfs path
}
  \item{hadoop.cmd}{
    hadoop command. default is "hadoop"
}
  \item{sep}{
    the field separator string
}
}
\details{
}
\value{
error_code
}
\references{
}
\author{
Yohei Sato
}
\note{
}

\seealso{
}
\examples{
\dontrun{
write.hdfs(iris,"tmp/iris")
}
}
\keyword{ ~kwd1 }

