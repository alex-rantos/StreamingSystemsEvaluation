import time
import sys
from my_db import MyDB
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import settings

# Global variable to identify when a script has finished.
# If IDLE_SECONDS == 100 delete all /rates/ contents and start a new query
IDLE_SECONDS = 0
DB = MyDB()

class Watcher:
    DIRECTORY_TO_WATCH = settings.MONITOR_DIRECTORY

    def __init__(self):
        self.observer = Observer()

    def run(self, args):
        global IDLE_SECONDS
        event_handler = Handler(args)
        self.observer.schedule(event_handler,
                               self.DIRECTORY_TO_WATCH,
                               recursive=True)
        self.observer.start()
        try:
            while True:
                seconds_to_sleep = 15
                IDLE_SECONDS += seconds_to_sleep
                time.sleep(seconds_to_sleep)
                if IDLE_SECONDS >= 90:
                    print(
                        "No modification in path for 2.5 minutes. Quitting monitorring..."
                    )
                    exit(0)
                if IDLE_SECONDS != 0:
                    print("TIME: " + str(IDLE_SECONDS))
        except:
            self.observer.stop()
            print("Error")

        self.observer.join()


class Handler(FileSystemEventHandler):
    def __init__(self, args):
        self.db = MyDB(args[0])
        self.args = args[1:]

    # on_created events happen only once. Delete /rates/ content for every new query
    # in order to make sure content is not written twice when using on_modified routive
    # def on_created(self, event):
    #     self.reset_timer()
    #     if event.is_directory:
    #         print("Directory was created.")
    #         return None
    #     elif event.event_type == 'created':
    #         # Taken any action here when a file is created.
    #         print("Received created event - [%s]." % event.src_path)
    #         self.db.add_file_contents(self.args, event.src_path)
    #     else:
    #         print("Unknown event: [%s] @ [%s]." %
    #               (event.event_type, event.src_path))

    def on_modified(self, event):
        self.reset_timer()
        if event.is_directory:
            print("Directory was modified.")
            return None
        elif event.event_type == 'modified':
            # Taken any action here when a file is modified.
            print("Received modified event - [%s]." % event.src_path)
            self.db.add_file_contents(self.args, event.src_path)
        else:
            print("Unknown event: [%s] @ [%s]." %
                  (event.event_type, event.src_path))

    def reset_timer(self):
        global IDLE_SECONDS
        IDLE_SECONDS = 0


def check_arguments(args):
    print(args)
    valid_arguments_per_query = settings.NUM_ARGUMENTS_PER_QUERY
    if (len(args) == 1):
        print("No arguments were given. Exiting " + args[0])
        exit(-1)
    if (args[2] not in DB.valid_query_names):
        print("Invalid argument [" + str(args[2]) + "] were given. Exiting " +
              args[0])
        exit(-1)
    # +1 for fileName (argv[0]), +1 for extension (argv[2]), + 1 for QueryName(args[2])
    if valid_arguments_per_query[args[2]] + 3 != len(args):
        print(args)
        print("Invalid numbers of arguments [#" + str(len(args)) +
              "] were given when only " +
              str(valid_arguments_per_query[args[2]]) +
              " are expected. Exiting " + args[0])
        exit(-1)


if __name__ == '__main__':
    check_arguments(sys.argv)
    w = Watcher()
    w.run(sys.argv[1:])
