import os
from file_watcher import Watcher

if __name__ == "__main__":
    # Ensure 'data' directory exists
    if not os.path.exists('data'):
        os.makedirs('data')
        print("Created 'data' directory for monitoring.")

    watcher = Watcher(path='data/')
    watcher.run()
