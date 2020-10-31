from nameko.rpc import rpc
from time import time
import datetime as dt


class TimeService:
    name = 'svc'

    @rpc
    def greeting(self, name):
        return f'hello {name}'
