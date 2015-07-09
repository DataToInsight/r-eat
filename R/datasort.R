
order.dataframe <- function(frame, decreasing=FALSE, ...){
  ordered <- order(frame[,ncol(frame)], decreasing=decreasing, ...)
  if (ncol(frame)>1){
    for(j in (ncol(frame)-1):1){
      if(decreasing){
        ordered=-ordered # using the order as secondary ordering reason. If decreasing, will flip the secondary ordering, thus flip it once extra in advance
      }
      # ordered holds ordering of columns j+1 .. ncol(frame)
      # need to translate that to an index for each row
      row_index <- rep(0, times=nrow(frame))
      row_index[ordered] <- 1:nrow(frame)
      ordered <- order(frame[,j], row_index, decreasing=decreasing, ...)
    }
  }
  return(ordered)
}
sort.dataframe <- function(frame, ...){
  frame[order.dataframe(frame),,drop=F]
}

columns.replace.and.add <- function(x, y){
  x<-data.frame(x)
  y<-data.frame(y)
  names.both <- prob:::intersect(colnames(x), colnames(y))
  names.new <- prob:::setdiff(colnames(y), colnames(x))
  if(length(names.both)>0){
    x[,names.both] <- y[,names.both,drop=F]
  }
  if(length(names.new)>0){
    x<-cbind(x,y[,names.new,drop=F])
  }
  return(x)
}

rbindflex <- function(a, b, default=NA){
  requireInSet(colnames(b), colnames(a), "Not all columns of b can be mapped to dataframe of a")
  mergeable<-data.frame(matrix(data=default, nrow=nrow(b), ncol=ncol(a),
                               dimnames=list(NULL,colnames(a))))
  mergeable[,colnames(b)]<-b
  rbind(a, mergeable)
}
