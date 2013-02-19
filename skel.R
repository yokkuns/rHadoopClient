
read.hdfs <- function(hdfs.path, hadoop.cmd = "hadoop") {
  tmp.file <- sprintf("tmp_hadoop_%s.csv", as.numeric(Sys.time()))
  cmd <- sprintf("%s fs -cat %s | perl -pe 's/\t/,/g' > %s", hadoop.cmd, hdfs.path, tmp.file)
  system(command = cmd)
  df <- read.csv(tmp.file, header = F, stringsAsFactors = F)
  file.remove(tmp.file)
  df
}

write.hdfs <- function(data, hdfs.path, hadoop.cmd = "hadoop",sep="\t") {
  tmp.file <- sprintf("tmp_hadoop_%s.csv", as.numeric(Sys.time()))
  write.table(data,file=tmp.file,row.names=F,quote=F,col.names=F,sep=sep)
  cmd <- sprintf("cat %s | %s fs -put - %s", tmp.file, hadoop.cmd, hdfs.path)
  result <- system(command = cmd)
  file.remove(tmp.file)
  result
}

read.hive <- function(sql, hive.cmd = "hive") {
  tmp.file <- sprintf("tmp_hive_%s.csv", as.numeric(Sys.time()))
  cmd <- sprintf("%s -e '%s' | perl -pe 's/\t/,/g' > %s", hive.cmd, sql, tmp.file)
  system(command = cmd)
  df <- read.csv(tmp.file, header = F, stringsAsFactors = F)
  file.remove(tmp.file)
  df
}

## package.skeleton(list=c("read.hdfs","write.hdfs","read.hive"),name="rhadoop_client")
