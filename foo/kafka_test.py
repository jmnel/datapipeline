from nameko_kafka import consume


class MyService:

    name = "my_service"

    @consume("test-event", group_id="my-group", bootstrap_servers="localhost:9092")
    def method(self, message):
        print(message.value)
