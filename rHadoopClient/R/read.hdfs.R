read.hdfs <-
function(hdfs.path, hadoop.cmd = "hadoop") {
  tmp.file <- sprintf("tmp_hadoop_%s.csv", as.numeric(Sys.time()))
  cmd <- sprintf("%s fs -cat %s | perl -pe 's/\t/,/g' > %s", hadoop.cmd, hdfs.path, tmp.file)
  system(command = cmd)
  df <- read.csv(tmp.file, header = F, stringsAsFactors = F)
  file.remove(tmp.file)
  df
}
