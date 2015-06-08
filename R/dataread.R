reat.csv2 <- function(path, na.strings=c(), stringsAsFactors = TRUE, check.names=F,  ...){
  result<-list()
  result$file <- basename(path)
  result$path <- path
  result$orig <- read.csv2(result$path, na.strings=na.strings, stringsAsFactors = stringsAsFactors, check.names=check.names, ...)
  return(result)
}

reat.csv <- function(path, na.strings=c(), stringsAsFactors = TRUE, check.names=F, row.names=NULL, ...){
  result<-list()
  result$file <- basename(path)
  result$path <- path
  result$orig <- read.csv(result$path, na.strings=na.strings, stringsAsFactors = stringsAsFactors, check.names=check.names, row.names=row.names, ...)
  return(result)
}
