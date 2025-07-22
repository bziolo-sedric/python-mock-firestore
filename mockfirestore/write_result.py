from mockfirestore._helpers import Timestamp

class WriteResult:
    def __init__(self):
        self.update_time = Timestamp.from_now()
