import time

import actors


@actors.remote
class Pinger(object):
    def set_up(self, pings, judge, ponger):
        self.pings_left = pings
        self.judge = judge
        self.ponger = ponger

        self.judge.ping_ready.remote()
        print("Ping Ready")

    def pong(self):
        if self.pings_left > 0:
            self.ponger.ping.remote()
            self.pings_left -= 1
        else:
            self.judge.finish.remote()


@actors.remote
class Ponger(object):
    def set_up(self, judge, pinger):
        self.pinger = pinger
        judge.pong_ready.remote()
        print("Pong Ready")

    def ping(self):
        self.pinger.pong.remote()


@actors.remote
class Judge(object):
    def set_up(self, num_pings, pinger, ponger):
        self.pings = num_pings
        self.pinger = pinger
        self.ponger = ponger
        self.pinger.set_up.remote(self.pings, self.proxy, self.ponger)
        self.ponger.set_up.remote(self.proxy, self.pinger)

        self.ping_ok = False
        self.pong_ok = False
        print("Judge Ready")

    def ping_ready(self):
        self.ping_ok = True
        self.run()

    def pong_ready(self):
        self.pong_ok = True
        self.run()

    def run(self):
        if self.ping_ok and self.pong_ok:
            self.init = time.time()
            self.pinger.pong.remote()
            print("First sent")

    def finish(self):
        self.end = time.time()
        total = (self.end - self.init)
        print(f"Did {self.pings} pings in {total} s")
        print(f"{self.pings / total} pings per second")

    def print(self):
        print(f"Printing Judge {self.key}")


if __name__ == '__main__':
    actors.start()
    judge = Judge.remote()
    pinger = Pinger.remote()
    ponger = Ponger.remote()

    judge.set_up.remote(100, pinger, ponger)

    time.sleep(10)
    actors.shutdown()
