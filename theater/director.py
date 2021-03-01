# import multiprocessing as mp
import lithops.multiprocessing as mp
from queue import Empty
from threading import Thread

director_queue = None


def send_stop(actor_key):
    if director_queue is None:
        # we are on the main process, where the director exists
        global global_director
        global_director.msg2(actor_key, 'pls stop')
    else:
        # we are on a subprocess, we have the director queue
        director_queue.put('pls stop')


def send_action(action):
    if director_queue is None:
        # we are on the main process, where the director exists
        global global_director
        global_director.msg2(action.actor_key, action)
    else:
        # we are on a subprocess, we have the director queue
        director_queue.put((action.actor_key, action))

    return None


def actor_process(actor_type, actor_key,
                  queue, director_q,
                  args, kwargs):
    # Create an instance without __init__ called.
    actor_class = actor_type
    actor_instance = actor_class.__new__(actor_class)
    # actor_instance.class_id = class_meta.class_id
    # actor_instance.proxy =

    actor_instance.key = actor_key
    actor_instance.__init__(*args, **kwargs)
    global director_queue
    director_queue = director_q

    while True:
        action = queue.get()
        # print(action)
        if action == 'pls stop':
            print(f"Stopping actor {actor_key}")
            break
        action.call(actor_instance)


class Director(object):

    def __init__(self):
        self.actors = {}
        self.queue = mp.Queue()

    def new_actor(self, class_meta, actor_key, args, kwargs):
        actor_queue = mp.Queue()
        self.actors[actor_key] = actor_queue
        actor_ps = mp.Process(target=actor_process,
                              args=(class_meta, actor_key,
                                    actor_queue, self.queue,
                                    args, kwargs))
        actor_ps.start()
        return actor_ps

    def run(self):
        def p():
            while self.running:
                try:
                    m = self.queue.get(timeout=1)
                    dest = m[0]
                    msg = m[1]
                    self.actors[dest].put(msg)
                except Empty:
                    pass

        self.running = True
        self.t = Thread(target=p)
        self.t.start()

    def stop(self):
        # stop all actors
        for actor in self.actors.keys():
            self.msg2(actor, 'pls stop')
        self.running = False
        self.t.join()

    def msg2(self, actor_key, msg):
        self.actors[actor_key].put(msg)


global_director = None


def start():
    global global_director
    if global_director is not None:
        print("Already started")
        return
    print("Starting Theater")
    global_director = Director()
    global_director.run()


def shutdown():
    global global_director
    if global_director is None:
        print("Not started, can't shutdown")
        return
    print("Stopping Theater director")
    global_director.stop()
    print("Shut down")


def new_actor(actor_key, meta, args, kwargs):
    global global_director
    if global_director is None:
        print("Not started, can't create actor")
    actor_type = meta.enriched_class.__thtr_actor_class__
    # proxy = meta.proxy_crafter
    global_director.new_actor(actor_type, actor_key, args, kwargs)
