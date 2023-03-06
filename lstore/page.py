
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < 512

    def write(self, value):
        num = value.to_bytes(8, 'big')
        for i in range(0, 8):
            self.data[(self.num_records*64)+i] = num[i]
        self.num_records += 1
        pass

