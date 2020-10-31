from nameko.extensions import DependencyProvider
from nameko.rpc import rpc


class Master:
    name = "master"

    @rpc
    def method(self):
        l = self.other_rpc.get_dependency()
        return l.method()


class Slave(DependencyProvider):
    name = "slave"

    def setup(self):
        pass


#container = ServiceContainer(Service, config={})

#service_extensions = list(container.extensions)

# container.start()
# container.stop()
