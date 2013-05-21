#!/bin/bash
rm -rf /home/rttest/experiments
cd /home/rttest/EEGTransfer/
java -jar EEGTransferCron.jar /home/rttest/
echo "Files for the iRODS were successfully created in " `/bin/date` >> eegTransfer.log
