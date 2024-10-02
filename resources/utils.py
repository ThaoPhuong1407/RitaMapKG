import time

id_index = 0

def get_time_millis():
    return time.time() * 1000

def make_id(id_prefix="id-"):
    global id_index
    id_index += 1
    return id_prefix + str(id_index)
