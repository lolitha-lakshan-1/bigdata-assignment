Docker compose ps


for i in {1..1000}; do 
  echo "sensor_data $i" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --topic traffic.raw
done

docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic traffic.raw \
  --group traffic-backend-service