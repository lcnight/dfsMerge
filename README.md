dfsMerge
========

simple merge file or push file to hdfs (>=1.0.1) in compression mode or not

Usage: <jarClass> [options]
    -m       src, one file or batch files(comma seperated) need to merge to dfs
    -f       dst, one file or batch files(comma seperated) match the target for src files
    -d       dst dir, one directory uri specify target dfs, cannot appear with [-f] options
    -c       denote merge log file running in compression mode
    -t       explicitly denote compression type, default is [bzip2], [-c] option must be set
    -h       show help info and exit
    -v       run in VERBOSE mode

current, this tool support bzip2/gzip/lzo/deflate compression type, and the compression 
type can be set by -t option


example: 
    # push two files to hdfs
    hadoop jar dfsMerge.jar dfsMerge -m log1,log3 -f log2,log4 -v

    # push two files to hdfs in compression and verbose mode
    hadoop jar dfsMerge.jar dfsMerge -m log1,log3 -f log2,log4 -v -c

contact info:
liuscmail#126.com
