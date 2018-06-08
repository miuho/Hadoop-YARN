ssh dijin@s-1.fgf.ClusterSched16.nome.nx
cd /groups/ClusterSched16/advcc9/719/bin
sudo ./net-slave.sh
sudo ./rackstart.sh r1 4G 2&

ssh dijin@s-2.fgf.ClusterSched16.nome.nx
cd /groups/ClusterSched16/advcc9/719/bin
sudo ./net-slave.sh
sudo ./rackstart.sh r2 4G 2&

ssh dijin@s-3.fgf.ClusterSched16.nome.nx
cd /groups/ClusterSched16/advcc9/719/bin
sudo ./net-slave.sh
sudo ./rackstart.sh r3 4G 2&

ssh dijin@s-4.fgf.ClusterSched16.nome.nx
cd /groups/ClusterSched16/advcc9/719/bin
sudo ./net-slave.sh
sudo ./rackstart.sh r4 4G 2&