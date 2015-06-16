# makes the data R compatible (unique non-empty column names)
preprocess.orig <- function(x){
	if(ncol(x)==0) {
		warning("preprocess.orig received 0 columns")
		return(x) #nothing to change
	}
  empty_colnames<- colnames(x)==""
  empty_colnames[is.na(empty_colnames)] <- T
  if(sum(empty_colnames, na.rm=T)>0){
    cat(paste0("Thera are ",sum(empty_colnames)," empty column names at indices ",paste0(collapse=", ",which(empty_colnames)),", they have been given name X with numeric sufixes:\r\n"))
    colnames(x)[empty_colnames]<-paste0("X",1:sum(empty_colnames)) 
  }
  colnames(x)<-gsub("\\.","",colnames(x)) #to make the names sql compatible
  colname_count <- table(tolower(colnames(x))) #sql is case insensitive
  for(c in names(colname_count[colname_count>1])){
    cat(paste0("There are ",colname_count[c]," columns with name '",c,"', they have been given numeric suffixes.  \r\n"))
    colnames(x)[which(tolower(colnames(x))==c)]<-paste0(c,1:colname_count[c])
  }
  return(x)
}

# prepares the data for usage, removes completely NULL rows & cols, makes colnames SQL compatible,
preprocess.data <- function(x, cols.keep=c(), cols.drop=c(), cols.map=list(), drop.cols=T, verbose=T){
  x<-filter.rows.empty(x)
  if( (length(drop.cols)>0 && drop.cols==T) 
	  || is.null(drop.cols)){
  	x <- filter.cols.empty(x, cols.keep, verbose)
  }

  if(length(cols.drop)>0){
	  if(verbose){
		  cat(paste0("Removing explicitly provided colnames ", paste(collapse=", ", cols.drop), "\r\n"))
	  }
	  x <- x[, !(colnames(x)%in%cols.drop), drop=F]
  }
  
  if(length(cols.map)>0){
      for(i in 1:length(cols.map)){
	  if(verbose){
	      cat(paste0("Renaming column ",names(cols.map)[i]," to ",cols.map[[i]], "\r\n"))
	  }
	 colnames(x)[colnames(x)==names(cols.map)[i]] <- cols.map[[i]]
      }
  }
  colnames(x) <- make.names.sql(colnames(x))
  x
}
