from nameko.cli import shell


class GreetingService:

    name = "greeting_service"

    @shell
    def hello(self, name):
        return 'Hello, {}!'.format(name)
