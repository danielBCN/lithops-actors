import lithops.multiprocessing as mp
# import multiprocessing as mp
import time
from queue import Empty
from threading import Thread


class Pinger(object):
    def set_up(self, pings, judge, ponger):
        self.pings_left = pings
        self.judge = judge
        self.ponger = ponger

        # self.judge.ping_ready()
        self.director(self.judge,
                      ('ping_ready', (), {}))
        print("Ping Ready")

    def pong(self):
        if self.pings_left > 0:
            # self.ponger.ping()
            self.director(self.ponger,
                          ('ping', (), {}))
            self.pings_left -= 1
        else:
            # self.judge.finish()
            self.director(self.judge,
                          ('finish', (), {}))


class Ponger(object):
    def set_up(self, judge, pinger):
        self.pinger = pinger
        # judge.pong_ready()
        self.director(judge,
                      ('pong_ready', (), {}))
        print("Pong Ready")

    def ping(self):
        # self.pinger.pong()
        self.director(self.pinger,
                      ('pong', (), {}))


class Judge(object):
    def set_up(self, num_pings, pinger, ponger):
        self.pings = num_pings
        self.pinger = pinger
        self.ponger = ponger
        # self.pinger.set_up(self.pings,'judge', self.ponger)
        self.director(self.pinger,
                      ('set_up', (self.pings, 'judge', self.ponger), {}))
        # self.ponger.set_up('judge', self.pinger)
        self.director(self.ponger,
                      ('set_up', ('judge', self.pinger), {}))

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
            # self.pinger.pong()
            self.director(self.pinger,
                          ('pong', (), {}))
            print("First sent")

    def finish(self):
        self.end = time.time()
        total = (self.end - self.init)
        print(f"Did {self.pings} pings in {total} s")
        print(f"{self.pings / total} pings per second")


def actor_process(actor_type, queue, director_queue):
    def send2director(name, msg):
        director_queue.put([name, *msg])

    instance = actor_type()
    instance.director = send2director
    while True:
        message = queue.get()
        # print(message)
        if message == 'pls stop':
            break
        method_name, args, kwargs = message
        getattr(instance, method_name)(*args, **kwargs)


class Director(object):
    def __init__(self):
        self.actors = {}
        self.queue = mp.Queue()

    def new_actor(self, actor_type, name):
        actor_queue = mp.Queue()
        self.actors[name] = actor_queue
        actor_ps = mp.Process(target=actor_process,
                              args=(actor_type, actor_queue, self.queue))
        actor_ps.start()
        return actor_ps

    def run(self):
        def p():
            while self.running:
                try:
                    m = self.queue.get(timeout=1)
                    dest = m[0]
                    msg = m[1:]
                    self.actors[dest].put(msg)
                except Empty:
                    pass

        self.running = True
        self.t = Thread(target=p)
        self.t.start()

    def stop(self):
        self.running = False
        self.t.join()

    def msg2(self, name, msg):
        self.actors[name].put(msg)


def main():
    director = Director()
    director.run()
    judge_ps = director.new_actor(Judge, 'judge')
    ping_ps = director.new_actor(Pinger, 'ping')
    pong_ps = director.new_actor(Ponger, 'pong')

    director.msg2('judge', ('set_up', (10, 'ping', 'pong'), {}))

    time.sleep(5)
    director.msg2('judge', ('pls stop'))
    director.msg2('ping', ('pls stop'))
    director.msg2('pong', ('pls stop'))
    judge_ps.join()
    ping_ps.join()
    pong_ps.join()
    director.stop()


if __name__ == '__main__':
    main()
