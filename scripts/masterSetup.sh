#!/bin/bash
##### Assume logged in MASTER NODE already ####

rsync -avz /proj/ClusterSched16/719 /groups/ClusterSched16/advccX/
bash; export PATH="/groups/ClusterSched16/advcc9/719/bin:$PATH"
cd ~ ; rm tetris.deth;
echo "m.fgf.ClusterSched16.nome.nx" >> tetris.deth;
echo "s-1.fgf.ClusterSched16.nome.nx" >> tetris.deth;
echo "s-2.fgf.ClusterSched16.nome.nx" >> tetris.deth;
echo "s-3.fgf.ClusterSched16.nome.nx" >> tetris.deth;
echo "s-4.fgf.ClusterSched16.nome.nx" >> tetris.deth;
tetris.sh

##### Only execute once ####
#rsync /proj/ClusterSched16/images/debian-current.qcow.GOLD /groups/ClusterSched16/advcc9/debian-current.qcow
cd /groups/ClusterSched16/advcc9/719/bin
sudo ./net-master.sh 9

sudo ./quickstart.sh 9&
echo "into sleep for 20 seconds."
sleep 20

##### In r0h0 ####
scp pathBashrc.txt root@10.9.1.10:~/.bashrc;
sudo su;
ssh root@10.9.1.10 mkdir -p .ssh;
ssh root@10.9.1.10;
ssh-keygen -t rsa;
cat ~/.ssh/id_rsa.pub >> .ssh/authorized_keys
addgroup --gid 6053 --force-badname Clu-advcc91
adduser --uid 10130 --gid 6053 dijin


sudo mkdir -p ~/.ssh/config
echo "Host *
	ForwardAgent yes" >> ~/.ssh/config
echo "dijin        ALL=NOPASSWD: ALL" >> /etc/sudoers
sed -i 's/10.10/10.9/g' /etc/fstab
/sbin/shutdown -h now
echo "into sleep for 20 seconds."
sleep 20

sudo ./net-master.sh 9
./rackstart.sh r0 8G 8&

./slaveSetup.sh&

#### Build up NFS ####
sudo su;
apt-get install nfs-kernel-server
mkdir -p /opt/projects/advcc
echo "/opt/projects/advcc 10.9.*.*(rw,sync,no_root_squash)" >> /etc/exports
service nfs-kernel-server start

#### Build Hadoop 2.x ####
cd ~
tar xvf hadoop-2.2-advcc-v2.tar.gz -C /opt/projects/advcc/

sudo chown -R dijin /opt/projects/
sudo chgrp -R Clu-advcc91 /opt/projects/

ssh dijin@10.9.1.10;
sudo chown -R dijin /opt/projects/
sudo chgrp -R Clu-advcc91 /opt/projects/
exit

cd /groups/ClusterSched16/advcc9/719/bin
./mpssh -s -f vmips "sudo reboot"

#### Change the rackcap ####
cd /groups/ClusterSched16/advcc9/719/bin
sed -i 's/rackcap = [1,6,6,6,6]/rackcap = [1,4,6,6,6]/g' gennetmaster.py

#### Change the host names ####
scp *_known_host.sh dijin@10.9.1.10:~/
ssh dijin@10.9.1.10
bash; ./build_known_hosts.sh; ./send_known_hosts.sh
exit

#### Change and run setup-scratch-user.sh ####
cp ~/setup-scratch-user.sh /opt/projects/advcc/hadoop/scripts/setup-scratch-user.sh
./mpssh -s -f vmips "cd /opt/projects/advcc/hadoop/scripts/; ./setup-scratch-user.sh"

#### Yarn master setup ####
cp ~/setup_yarn_master.sh /opt/projects/advcc/
ssh dijin@10.9.1.10
cd /opt/projects/advcc/; ./setup_yarn_master.sh
exit

#### Yarn slaves setup ####
cp ~/setup_yarn_slaves.sh /opt/projects/advcc/
./mpssh -f -s slavevmips "cd /opt/projects/advcc/; ./setup_yarn_slaves.sh"



