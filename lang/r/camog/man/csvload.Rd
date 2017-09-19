\name{csvload}
\alias{csvload}
\title{Data Input}
\description{
  Reads a csv file.
}
\details{
  Reads csv files using multiple threads.
}
\keyword{file}
\seealso{
\code{\link{read.table}}
}
\examples{
test1 <- c("a,b,c", 1:5, "6,7", "8.0,9,10.0")
tf <- tempfile()
writeLines(test1, tf)

csvload(tf)

unlink(tf)
}
