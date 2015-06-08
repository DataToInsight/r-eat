filter.rows <- function(x, keep, col.name, value, type, ...){
  if(length(col.name)==0){
    return(filter.rows(x, keep, colnames(x), value, type))
  } else if(length(col.name)>1) {
    for(col.name in col.name){
      x <- filter.rows(x, keep, col.name, value, type)
    }
    return(x)
  } else{
    requireInSet(col.name, colnames(x), "filtered column")
    matches <- switch(type,
                        regex={
                          grepl(value, x[,col.name], ...)
                        },
                        is.na={
                          is.na(x[,col.name])
                        })
    selection <- if(keep){
      matches
    } else{
      !matches
    }
    return(x[selection,])
  }
}

# remove rows which are all na
filter.rows.empty <- function(x, verbose=T){
  if(verbose & sum(rowSums(na_or_empty_vals(x))>=ncol(x))){
    cat("Removed rows ", which(rowSums(na_or_empty_vals(x))>=ncol(x)), " because they contain only NA and empty values.  \r\n")
  }
  x <- x[rowSums(na_or_empty_vals(x))<ncol(x),,drop=F]
  return(x)
}

# remove columns which are all na
filter.cols.empty <- function(x, cols.keep=NULL, verbose=T){
  all_na <- sapply(x, function(v)sum(is.na(v))==length(v))
  if(length(cols.keep)>0){
    requireInSet(cols.keep, colnames(x), "column names to keep even if they are null")
    overruled <- all_na & is.element(colnames(x), cols.keep)
    if(verbose & sum(overruled)>0)
      print(paste0("Columns '", paste(collapse="', '", colnames(x)[overruled]), "' only contain <NA> values but are not removed because they are overruled."))
    all_na <- all_na & !is.element(colnames(x), cols.keep)
  }
  if(verbose & sum(all_na)>0)
    print(paste0("Columns with only <NA> values are removed : ", paste(collapse=", ", colnames(x)[all_na])))
  x <- x[,!all_na,drop=F]
  return(x)
}
