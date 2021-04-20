import logging

import lithops.multiprocessing as mp

actor_directory = {}

logger = logging.getLogger(__name__)


def send_stop(actor_key):
    if actor_directory:
        # we are on a subprocess, we have the directory
        actor_directory[actor_key].put('pls stop')
    else:
        # we are on the main process, where the director exists
        global global_director
        global_director.msg2(actor_key, 'pls stop')


def send_action(action):
    if actor_directory:
        # we are on a subprocess, we have the director queue
        actor_directory[action.actor_key].put(action)
    else:
        # we are on the main process, where the director exists
        global global_director
        global_director.msg2(action.actor_key, action)
        # print(f"Put action {action}")

    return None


def actor_process(actor_type, weak_ref,
                  queue, directory, event,
                  args, kwargs):
    # Create an instance without __init__ called.
    actor_class = actor_type
    actor_instance = actor_class.__new__(actor_class)
    actor_instance.class_id = weak_ref._thtr_class_id
    actor_instance.proxy = weak_ref.build_proxy()

    actor_instance.key = weak_ref._thtr_actor_key
    actor_instance.__init__(*args, **kwargs)
    global actor_directory
    actor_directory = directory

    event.set()  # tell father i'm ready
    while True:
        action = queue.get()
        # print(action)
        if action == 'pls stop':
            logger.debug(f"Stopping actor {weak_ref._thtr_actor_key}")
            break
        action.call(actor_instance)


class Director(object):

    def __init__(self):
        # self.actors = {}
        # self.queue = mp.Queue()
        self.manager = mp.Manager()
        self.actors = self.manager.dict()

    def new_actor(self, actor_type, weak_ref, args, kwargs):
        actor_queue = mp.Queue()
        self.actors[weak_ref._thtr_actor_key] = actor_queue
        event = self.manager.Event()
        actor_ps = mp.Process(target=actor_process,
                              args=(actor_type, weak_ref,
                                    actor_queue, self.actors, event,
                                    args, kwargs))
        actor_ps.start()
        # we need to wait for the child to be working,
        # otherwise, the queue loses events
        event.wait()
        return actor_ps

    def run(self):
        # def p():
        #     while self.running:
        #         try:
        #             m = self.queue.get(timeout=1)
        #             dest = m[0]
        #             msg = m[1]
        #             self.actors[dest].put(msg)
        #         except Empty:
        #             pass
        #
        self.running = True
        # self.t = Thread(target=p)
        # self.t.start()

    def stop(self):
        # stop all actors
        for actor in self.actors.keys():
            self.msg2(actor, 'pls stop')
        self.running = False
        # self.t.join()

    def msg2(self, actor_key, msg):
        self.actors[actor_key].put(msg)


global_director = None


def start():
    global global_director
    if global_director is not None:
        logger.info("Already started")
        return
    logger.info("Starting Lithops Actors")
    global_director = Director()
    global_director.run()


def shutdown():
    global global_director
    if global_director is None:
        logger.info("Not started, can't shutdown")
        return
    logger.info("Stopping Lithops Actors director")
    global_director.stop()
    logger.info("Shut down")


def new_actor(meta, weak_ref, args, kwargs):
    global global_director
    if global_director is None:
        raise Exception("Not started, can't create actor")
        # TODO: actors cannot be created from other actors
    actor_type = meta.enriched_class.__thtr_actor_class__
    global_director.new_actor(actor_type, weak_ref, args, kwargs)
