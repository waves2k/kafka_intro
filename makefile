
build-consumer:
	@go build -o ./consumer/bin/consumer ./consumer/consumer.go

build-producer:
	@go build -o ./producer/bin/producer ./producer/producer.go

consumer: build-consumer
	@./consumer/bin/consumer

producer: build-producer
	@./producer/bin/producer

docker-up:
	docker-compose-up -d

docker-stop:
	docker-compose down