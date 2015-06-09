# Convenience function to change datatype to boolean, throws exception if unexpected values are found. Provide single/vectors of expected True, False, NA values.
as.boolean <- function(x, TrueValue, FalseValue, NAValue=c()){
  # Verify that there aren't multiple mappings for one value in x (to True / False / NA)
  all_vals <- c(TrueValue, FalseValue, NAValue)
  if(length(all_vals)>length(unique(all_vals))){
    val_count <- table(all_vals)
    stop(paste0("The values ", paste0(collapse=", ", names(val_count)[val_count>1] )," are defined to become multiple boolean values (T/F/NA)."))
  }
  # Verify that all values in x are mapped
  x<-as.character(x)
  TrueValue <- as.character(TrueValue)
  FalseValue <- as.character(FalseValue)
  NAValue <- as.character(NAValue)
  unexpected_values <- setdiff(unique(x), c(TrueValue, FalseValue, NAValue))
  if(length(unexpected_values)>0) stop(paste0("Did not expect values ",paste(unexpected_values, collapse = ",")))
  
  y <- x%in%TrueValue
  y[x%in%FalseValue] <- FALSE
  y[x%in%NAValue] <- NA
  return(y)
}

# replaces all values in x that occur in provided NAvalues with NA
as.na <- function(x, NAvalues){
  x[x%in%NAvalues]<-NA
  if(is.factor(x)){
    all_levels<-levels(x)
    x<-factor(x, levels = all_levels[!all_levels%in%NAvalues])
  }
  return(x)
}

# Returns the character/factor columns which contain commas between digits, could be worth to try to convert them to numerics after removing the commas
columns.possiblynumeric <-function(x, thousands.separator=","){
  if(thousands.separator==".")
    thousands.separator<-"\\."
  return(sapply(x, function(x){
    (is.character(x) | is.factor(x)) & # The columns of character type
      (
        (sum(!is.na(suppressWarnings(as.numeric(as.character(x)))))>0) # The columns which contain numeric numbers
         |
        (sum(grepl(pattern = paste0("([0-9])",thousands.separator,"([0-9])"), x = as.character(x)))>0) # The columns containing , between digits
      )
  }))
} 

vector.convert <- function(v, xname=NULL, convertname=NULL, convert.func, verbose=T, test=T){
  xname_descr <- if(length(xname)>0)paste0(xname, " ") else ""
  convert_descr <- if(length(convertname)>0)paste0("to ",convertname, " ") else ""
  v_new<-convert.func(v)
  non_empties <- !is.na(v) & nchar(as.character(v))>0
  failed_conversions <- is.na(v_new) & non_empties # the nonempty which did not convert ok
  if(sum(failed_conversions)>0){
	too_many_failures <- (sum(!is.na(v_new))==0 |
         sum(failed_conversions)>2*sum(!is.na(v_new)))
    if(test && too_many_failures){
      failure_count_msg <- paste0("Did not convert column ", xname_descr, convert_descr," because of too many failures (", sum(failed_conversions),") compared to successes (", sum(!is.na(v_new)),")")
      v_new <- v #restore original
      if(verbose){
        cat(paste0(failure_count_msg,"\r\n"))
      }else{
      	warning(failure_count_msg)
      }
    }else{
      failure_msg <- paste0("Converted column ", xname_descr, convert_descr," but had ", sum(failed_conversions), " failed conversions. ",
                            "The unique values:'", paste0(collapse="','",unique(v[failed_conversions])),"'")
      if(verbose){
        cat(paste0(failure_msg,"\r\n"))
      }else{
      	warning(failure_msg)
      }
    }
  }else{
    if(verbose){
      cat(paste0("Converted column ", xname_descr, convert_descr," with no failures on ",sum(non_empties)," nonempty values.  "))
      failed_non_nulls <- is.na(v_new)&!is.na(v)
      if(sum(failed_non_nulls)>0)
        cat(paste0(" All unique failures: '", paste0(collapse="', '",unique(v[failed_non_nulls])),"'.  \r\n"))
      else
        cat("\r\n")
    }
  }
  return(v_new)
}

# Given a dataframe, tries to transform character/factor columns to numeric by removing thousands separators.
# If the number of failures is reasonably small the column will be made numeric, else it will remain unmodified
columns.trynumeric <- function(x, thousands.separator=",", verbose=F, test=T){
  candidate_numeric_columns <- if(test){
	  candidate_numeric_columns <- columns.possiblynumeric(x, thousands.separator)
	  candidate_numeric_columns <- names(candidate_numeric_columns)[candidate_numeric_columns]  
	  candidate_numeric_columns
  }else{
	  colnames(x)
  }
  for(name in candidate_numeric_columns){
    x[,name] <- vector.convert(x[,name], xname=name, convertname="numeric", verbose=verbose, function(v)as.numeric(gsub(",","", as.character(v))), test)
  }
  return(x)
}

columns.try.POSIXct <- function(x, format, alt_suffix=c(), alt_format=c(), failedsuffix="_failed", origsuffix="_orig", verbose=T, test=T){
  columns.try.parse(x, format, 
                    parser = as.POSIXct,
                    passer.name = "POSIXct",
                    alt_suffix, alt_format, failedsuffix, origsuffix, verbose, test)
}


columns.try.Date <- function(x, format, alt_suffix=c(), alt_format=c(), failedsuffix="_failed", origsuffix="_orig", verbose=T, test=T){
  columns.try.parse(x, format, 
                    parser = as.Date,
                    passer.name = "Date",
                    alt_suffix, alt_format, failedsuffix, origsuffix, verbose, test)
}
  
columns.try.parse <- function(x, format, parser, passer.name, alt_suffix=c(), alt_format=c(), failedsuffix="_failed", origsuffix="_orig", verbose=T, test=T){
  stopifnot(is.character(format))
  stopifnot(length(alt_suffix)==length(alt_format))
  
  #TODO!:if(verbose){
  #  cat(paste0("Following columns have been identified as dates matching ", grep_pattern, ":\r\n"))
  #  print(candidate_columns)
  #}
  #result <- data.frame(colname=NA_character_, successes=)
  for(name in colnames(x)){
    x_orig <- x[,name]
    x_test <- suppressWarnings(parser(as.character(x[,name]), format=format)) # silent test to get nr of successes
    x_test_successes <- sum(!is.na(x_test))
    if(!test || x_test_successes>0){ # forced to do all provided columns, or some values succeeded
      x_new <-vector.convert(x[,name], xname=name, convertname=paste0(passer.name, " (format=",format,")"), 
                             function(v)suppressWarnings(parser(as.character(v), xname=name, format=format)), # verbose actual conversion to get logs of failures
                             verbose=T, test=test) 
      failed <- x_orig
      failed[!is.na(x_new)]<-NA
      x[,name]<-x_new
      
      if(sum(!is.na(failed))>0){ # some values failed to be converted
        #TODO: if the format matches only part of the string, the remainder is ignored. Needs to be put somewhere else(orig)
        if(length(alt_suffix)>0){
          for(i in seq_along(alt_suffix)){
            alt_name <- paste0(name, alt_suffix[i])
            alt_pattern <- alt_format[i]
            x_test <- suppressWarnings(parser(as.character(failed[!is.na(failed)]), format=alt_pattern)) # silent test to get nr of successes
            if(sum(!is.na(x_test))>0){ # this pattern could work
              msg<-paste0("As there were ",sum(!is.na(failed))," additional NA values resulting from parsing '",name,"' as date with format '",format,"', ",
                          "an additional column '",alt_name,"' will be created which contains the values parsed using '",alt_pattern,"'")
              if(verbose){
                print(msg)
              }else{
		  warning(msg)
	      }
              x[,alt_name] <- vector.convert(x_orig, xname=alt_name, convertname=passer.name, 
                                             function(v)suppressWarnings(parser(as.character(v), xname=name, format=alt_pattern, verbose=verbose)), test=test) 
              failed[!is.na(x[,alt_name])] <- NA # Remove the new ones that succeeded using this pattern
            }
          }
        }
        if(sum(!is.na(failed))>0 & length(failedsuffix)>0 & nchar(failedsuffix)>0){
          x[,paste0(name, failedsuffix)] <- failed
        }
      }
      if(length(origsuffix)>0 & nchar(origsuffix)>0 & sum(as.character(x_new[!is.na(x_new)])!=as.character(x_orig[!is.na(x_new)]))){
        x[,paste0(name, origsuffix)] <- x_orig
      }
    }
  }
  return(x)
}

as.Date.verbose <- function(x, verbose=T, xname=NULL, ... ){
  date_vector <- as.Date(x, ...)
  if(sum(is.na(date_vector))>0){
    cat("The following values",if(length(xname)>0)paste0("of ",xname) else "","have been transformed to NA date values \r\n")
    print(unique(as.character(x[is.na(date_vector)])))
    cat("The date conversion was performed with the following arguments:\r\n")
    print(list(...))
  }
  return(date_vector)
}

as.POSIXct.verbose <- function(x, verbose=T, xname=NULL, ...){
  datetime_vector <- as.POSIXct(x, ...)
  if(sum(is.na(datetime_vector))>0){
    cat("The following values ",if(length(xname)>0)paste0("of ",xname) else "","have been transformed to NA datetime values:\r\n")
    print(unique(as.character(x[is.na(datetime_vector)])))
    cat("The datetime conversion was performed with the following arguments:\r\n")
    print(list(...))
  }
  return(datetime_vector)
}


na_or_empty_vals <- function(x){ 
  if(nrow(x)==0){
    return(logical())
  }
  nas <- is.na(x)
  emptys <- apply(x,1,function(x){nchar(as.character(x))})==0
  nas_or_emptys <- if(ncol(x)>1){ 
      nas | t(emptys)
    }else{
      nas | emptys
    }
  return(nas_or_emptys)
}
