# vagrant comandos
vagrant up  -> Comando para prender la maquina virtual
vagrant ssh -> Entrar a la maquina virtual
vagrant halt -> apagar la maquina virtual
# Otros comandos
jps     =====>>>Ver el status si es q los servicios estan activos
# Activar servicios hdfs y yarn
start-dfs.sh
start-yarn.sh
# En caso de reinicio de servicios
stop-dfs.sh
stop-yarn.sh

# #####INSTALACION MAQUINA VIRTUAL########
# INSTALAR VAGRANT PRIMERO LUEGO clonar repositorio
git clone https://github.com/dgadiraju/itversity-boxes.git
cd itversity-boxes\centos7spark\
vagrant up
vagrant ssh
# ####INSTALACION HIVE#####
wget http://archive.apache.org/dist/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz
wget https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar
#opcional mover desde carpeta compartida (vagrant)##
cd /vagrant
# dejar un solo hadoop en la capeta /opt
# mover hadoop-3.* a otro lado y eliminar hadoop
# reemplazar hadoop-3.* por hadoop (faltan comandos)
mv apache-hive-3.1.2-bin.tar.gz /opt/hadoop
cd /opt/hadoop

tar xzf apache-hive-3.1.2-bin.tar.gz
mv apache-hive-3.1.2-bin hive
rm apache-hive-3.1.2-bin.tar.gz
chown -R vagrant hive

export HIVE_HOME=/opt/hadoop/hive
export PATH=$HIVE_HOME/bin:$PATH

cd /opt/hadoop/hive
$HADOOP_HOME/bin/hadoop fs -mkdir /tmp
hadoop fs -mkdir /user/hive
#probar sin el comando anterior (se agrego -p)
$HADOOP_HOME/bin/hadoop fs -mkdir -p /user/hive/warehouse
$HADOOP_HOME/bin/hadoop fs -chmod g+w /tmp
$HADOOP_HOME/bin/hadoop fs -chmod g+w /user/hive/warehouse

cd /opt/hadoop/hive/lib
rm guava-19.0.jar
cd /vagrant
mv guava-27.0-jre.jar /opt/hadoop/hive/lib
$HIVE_HOME/bin/schematool -dbType derby -initSchema

# Confirmar que funciona sino reiniciar servicios
schematool -dbType derby -info
