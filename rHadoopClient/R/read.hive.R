read.hive <-
function(sql, hive.cmd = "hive") {
  tmp.file <- sprintf("tmp_hive_%s.csv", as.numeric(Sys.time()))
  cmd <- sprintf("%s -e '%s' | perl -pe 's/\t/,/g' > %s", hive.cmd, sql, tmp.file)
  system(command = cmd)
  df <- read.csv(tmp.file, header = F, stringsAsFactors = F)
  file.remove(tmp.file)
  df
}
