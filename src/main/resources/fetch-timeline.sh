#!/bin/bash
LOGFILE=github-timeline.log
for (( i=0; i<1000000; i++ ))
do
  curl -o- -i 'https://github.com/timeline.' >> $LOGFILE
  sleep 1
done
