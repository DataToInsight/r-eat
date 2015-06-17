require(reader)
regcount <- function(...){
 sapply(gregexpr(...), length)
}

# uses reader's get.delim, but ignores trailing empty lines (which cause get.delim to not return any delimiter)
get.delimiter <- function(fn, n =10, skip = 0, ...){
	headlines <- readLines(fn, n=skip+n)
	headlines <- headlines[nchar(headlines)>0]
	n_available <- length(headlines)-skip
	if(n_available<1)
		stop("file has less (nonempty) lines than skipped")
	#get.delim also doesn't take care of quoted strings (containing delimiters)
	sep <- get.delim(fn, skip=skip, n=n_available, ...)
	sep
}

# determines itself wether to reat.csv or reat.csv2
reat.csv <- function(path, skip=0, ...){
	sep <- get.delimiter(path, n=1, skip=0, delims=c(';',','))
	if(length(sep)!=1)
		stop("no csv delimiter found")
	else if(sep==';')
		read.csv2(path, skip=skip, ...)
	else if (sep==',')
		read.csv(path, skip=skip, ...)
}

