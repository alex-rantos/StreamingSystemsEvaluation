cp /flink/flink-1.4.1/conf/flink-conf.yaml /flink/flink-1.4.1/conf/flink-conf.yaml_backup
sed -i "s/taskmanager.numberOfTaskSlots:.*/taskmanager.numberOfTaskSlots: 16/g" /flink/flink-1.4.1/conf/flink-conf.yaml
sed -i "s/# state.backend: .*/state.backend: filesystem/g" /flink/flink-1.4.1/conf/flink-conf.yaml
sed -i "s/# state.backend.fs.checkpointdir: .*/state.backend.fs.checkpointdir: file:\/\/\/path\/to\/saves/g" /flink/flink-1.4.1/conf/flink-conf.yaml
/flink/flink-1.4.1/bin/start-cluster.sh
echo "View the dashbord at : http://localhost:8081/#/overview"
