# include rmr2
library(rmr2)

# input csv file
train<-read.csv(file="small.csv",header=FALSE)
names(train)<-c("user","item","pref")

# 使用 rmr 的 hadoop 格式(default)。
rmr.options(backend = 'hadoop')

#put data to HDFS
train.hdfs = to.dfs(keyval(train$user,train))
# from.dfs(train.hdfs)

# STEP 1.1 建立 item's co-occurrence matrix
train.mr<-mapreduce(
  train.hdfs, 
  map = function(k, v) {
    keyval(k,v$item)
  },
  reduce = function(k,v){
    m<-merge(v,v)
    keyval(m$x,m$y)
  }
)

# STEP 1.2 算出 item's co-occurrence matrix frequence
step2.mr<-mapreduce(
  train.mr,
  map = function(k, v) {
    d<-data.frame(k,v)
    d2<-ddply(d,.(k,v),count)

    key<-d2$k
    val<-d2
    keyval(key,val)
  }
)

# STEP 2 建立 user's 評分矩陣
train2.mr<-mapreduce(
  train.hdfs, 
  map = function(k, v) {
    #df<-v[which(v$user==3),]
    df<-v
    key<-df$item
    val<-data.frame(item=df$item,user=df$user,pref=df$pref)
    keyval(key,val)
  }
)

# STEP 3 equijoin co-occurrence matrix and score matrix
eq.hdfs<-equijoin(
  left.input=step2.mr, 
  right.input=train2.mr,
  map.left=function(k,v){
    keyval(k,v)
  },
  map.right=function(k,v){
    keyval(k,v)
  },
  outer = c("left")
)

# STEP 4 計算推薦的結果
cal.mr<-mapreduce(
  input=eq.hdfs,
  map=function(k,v){
    val<-v
    na<-is.na(v$user.r)
    if(length(which(na))>0) val<-v[-which(is.na(v$user.r)),]
    keyval(val$k.l,val)
  },
  reduce=function(k,v){
    val<-ddply(v,.(k.l,v.l,user.r),summarize,v=freq.l*pref.r)
    keyval(val$k.l,val)
  }
)

# STEP 5 output list and score
result.mr<-mapreduce(
  input=cal.mr,
  map=function(k,v){
    keyval(v$user.r,v)
  },
  reduce=function(k,v){
    val<-ddply(v,.(user.r,v.l),summarize,v=sum(v))
    val2<-val[order(val$v,decreasing=TRUE),]
    names(val2)<-c("user","item","pref")
    keyval(val2$user,val2)
  }
)

from.dfs(result.mr)