#!/bin/sh
#harmless to run on existing dir. Run on all nodes in a new vm cluster
#For Namenode : follow up with bin/hadoop namenode -format
USR=dijin    #this should equal to `id -u`
GRP=Clu-advcc91  #this should equal to `id -g`
sudo mkdir -p /mnt/scratch/${USR}
sudo chown -R ${USR}:${GRP} /mnt/scratch/${USR}
#set sticky bit
sudo chmod -R g+s /mnt/scratch/${USR}