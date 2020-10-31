from nameko.timer import timer


class ServiceA:
    name = "service_a"

    @timer(interval=5)
    def hi(self):
        print('hi')
