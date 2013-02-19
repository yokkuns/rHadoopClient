write.hdfs <-
function(data, hdfs.path, hadoop.cmd = "hadoop",sep="\t") {
  tmp.file <- sprintf("tmp_hadoop_%s.csv", as.numeric(Sys.time()))
  write.table(data,file=tmp.file,row.names=F,quote=F,col.names=F,sep=sep)
  cmd <- sprintf("cat %s | %s fs -put - %s", tmp.file, hadoop.cmd, hdfs.path)
  result <- system(command = cmd)
  file.remove(tmp.file)
  result
}
