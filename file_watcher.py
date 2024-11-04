import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from file_utils import process_file

class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            process_file(event.src_path)

class Watcher:
    def __init__(self, path='data/'):
        self.observer = Observer()
        self.path = path

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.path, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()
