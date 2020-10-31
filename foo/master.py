from nameko.rpc import rpc, RpcProxy


class SlaveService:
    name = "slave"

    @rpc
    def method(self):
        return "hi"


class MasterService:
    name = "master"

    slave_rpc = RpcProxy("slave")

    @rpc
    def method(self):
        return self.slave_rpc.method()
