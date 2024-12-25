from mylogger import log
from mylogger import MyLogger

@log
def foo(a, b, logger):
    pass

@log
def bar(a, b=10, logger=None): # Named parameter
    pass

foo(10, 20, MyLogger())  # OR foo(10, 20, MyLogger().get_logger())
bar(10, b=20, logger=MyLogger())  # OR bar(10, b=20, logger=MyLogger().get_logger())
class Foo:
    def __init__(self, logger):
        self.lg = logger

    @log
    def sum(self, a, b=10):
        return a + b

Foo(MyLogger()).sum(10, b=20)  # OR Foo(MyLogger().get_logger()).sum(10, b=20)

print(MyLogger())
