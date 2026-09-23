## Santa 2016 prototype


library(data.table)

### distributions of gifts weigts
horse <-  function(){
  x <- pmax(rnorm(1000000,5,2), 0)
  return (x)
} 

ball<-  function(){
  x <- pmax(1 + rnorm(1000000,1,0.3), 0)
  return (x)
} 

bike<-  function(){
  x <- pmax(rnorm(1000000,20,10), 0)
  return (x)
} 

train<-  function(){
  x <- pmax(rnorm(1000000,10,5), 0)
  return (x)
} 

coal<-  function(){
  x <- 47 * rbeta(1000000,0.5,0.5)
  return (x)
} 

book<-  function(){
  x <- rchisq(1000000, 2)
  return (x)
} 

doll<-  function(){
  x <- rgamma(1000000, 5, 1)
  return (x)
} 

block<-  function(){
  a=5
  b=20
  c=10
  u = runif(1000000)
  fc = (c-a)/(b-a)
  output = a + sqrt(u*(b-a)*(c-a))
  output[u>=fc] <- b - sqrt((1-u[u>=fc])*(b-a)*(b-c))
  return (output)
} 

gloves<-  function(){
  test = runif(1000000)
  output = runif(1000000)
  output[test<0.3] = output[test<0.3] + 3
  return(output)
} 

# precalculate and save - faster than calculating each time
# important - befor each use it needs to be reshuffled 
distributions <- data.table(horsesD=horse(),
                            ballsD=ball(),
                            bikesD=bike(),
                            trainsD=train(),
                            coalsD=coal(),
                            booksD=book(),
                            dollsD=doll(),
                            blocksD=block(),
                            glovessD=gloves()
                            )


cost <- seq(0.05,200,0.1)
cost[cost>50] <- 0

# calculate score for each bag
score_one_row <- function(one_row,plot=FALSE){
  x<- rep(0,1000000)
  for (col in names(one_row)){
    if (one_row[,col,with=F]>0){
      for(n in seq(one_row[,col,with=F])){
        x <- x + sample(as.matrix(distributions[,paste(col,'D',sep=""),with=F]),nrow(distributions),replace = F)
      }
    }
  }
  d <- 0.1
  h <- hist(x,breaks = seq(0,200,d),plot=plot)
  if (plot){
    plot(h$density*d * cost)
  }
  return (sum(h$density*d * cost))
}

# 1000x9 data.table to store information about bags
sub <- data.table(horses=rep(0,1000),
                  balls=rep(0,1000),
                  bikes=rep(0,1000),
                  trains=rep(0,1000),
                  coals=rep(0,1000),
                  books=rep(0,1000),
                  dolls=rep(0,1000),
                  blocks=rep(0,1000),
                  glovess=rep(0,1000)
                  )

# unpacked gifts
store <- data.table(horses=1000,
                  balls=1100,
                  bikes=500,
                  trains=1000,
                  coals=166,
                  books=1200,
                  dolls=1000,
                  blocks=1000,
                  glovess=200
                  )


######################
## optimization loop
for (n in seq(1000)){
  row_n_left <- round(runif(1,1,1000)) # select bag
  row_left <- sub[row_n_left,] 
  row_right <- store # swapping between bag and unpacked gifts
  
  # moving from bag to store
  n_left <- round(runif(1,0,4)) # select number of gifts to swap
  if (n_left>0){
    for (m1 in seq(n_left)){
      ncol <- round(runif(1,1,9)) # selct gift to swap and swap if possible
      if (row_left[,ncol,with=F]>0){
        row_left[,ncol] <- row_left[,ncol,with=F] -1
        row_right[,ncol] <- row_right[,ncol,with=F] +1
      }
    }
  }
  # moving from store to bag
  n_right <- round(runif(1,0,4))
  if (n_right>0){
    for (m1 in seq(n_right)){
      ncol <- round(runif(1,1,9))
      if (row_right[,ncol,with=F]>0){
        row_right[,ncol] <- row_right[,ncol,with=F] -1
        row_left[,ncol] <- row_left[,ncol,with=F] +1
      }
    }
  }
  # check if new score is better
  base_score <- score_one_row(sub[row_n_left ,],plot=FALSE)
  new_score <- score_one_row(row_left,plot=FALSE)
  if (!all(sub[row_n_left ,]==row_left)){
    if (new_score> base_score){
      print(new_score-base_score)
      print(sub[row_n_left,])
      print(row_left)
      print(store)
      print(row_right)    
      sub[row_n_left,] <- row_left
      store <- row_right
    }
  }
  
# do the same but swapping between two bags
########  
  row_n_left <- round(runif(1,1,1000))
  row_n_right <- round(runif(1,1,1000))
  row_left <- sub[row_n_left,]
  row_right <- sub[row_n_right,]
  n_left <- round(runif(1,0,4))
  if (n_left >0){
    for (m1 in seq(n_left)){
      ncol <- round(runif(1,1,9))
      if (row_left[,ncol,with=F]>0){
        row_left[,ncol] <- row_left[,ncol,with=F] -1
        row_right[,ncol] <- row_right[,ncol,with=F] +1
      }
    }
  }

  n_right <- round(runif(1,0,4))
  if (n_right>0){
    for (m1 in seq(n_right)){
      ncol <- round(runif(1,1,9))
      if (row_right[,ncol,with=F]>0){
        row_right[,ncol] <- row_right[,ncol,with=F] -1
        row_left[,ncol] <- row_left[,ncol,with=F] +1
      }
    }
  }
  base_score <- score_one_row(sub[row_n_left ,],plot=FALSE) + score_one_row(sub[row_n_right ,],plot=FALSE)
  new_score <- score_one_row(row_left,plot=FALSE) + score_one_row(row_right,plot=FALSE)
  if (!all(sub[row_n_left ,]==row_left)){
    if (new_score> base_score){
      print(new_score-base_score)
      print(sub[row_n_left,])
      print(row_left)
      print(sub[row_n_right,])
      print(row_right)    
      sub[row_n_left,] <- row_left
      sub[row_n_right,] <- row_right
    }
  }
###############
  # colSums(sub) + store
}

moo <- sub[, score_one_row(.SD), by = 1:nrow(sub)]
total_score <- sum(moo$V1) # 

