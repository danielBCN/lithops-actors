import time

import actors


@actors.remote
class Counter(object):
    def __init__(self):
        self.value = 0
        print("Run init...")
        print(f"Actor key: {self.key}")

    def increment(self):
        self.value += 1
        print(f"Run inc {self.value}")
        return self.value

    def get_counter(self):
        print(f"Run get {self.value}")
        return self.value

    def set_self(self, proxy):
        print(proxy)
        proxy.get_counter.remote()

    def check_proxy(self):
        # proxy = actors.role(Counter).for_key(self.key)
        proxy = Counter.for_key(self.key)
        print(proxy)
        proxy.get_counter.remote()


def main():
    actors.start()
    # counter_actor = actors.role(Counter).remote()
    counter_actor = Counter.remote()

    [counter_actor.increment.remote() for _ in range(10)]

    # counter_actor.pls_stop()
    count = counter_actor.get_counter.future.remote()  # Returns do not work yet

    print(f"Count: {count}")    # This will be count.get(), since it's a future

    counter_actor.set_self.remote(counter_actor)
    counter_actor.check_proxy.remote()

    time.sleep(5)
    actors.shutdown()


if __name__ == '__main__':
    main()
