import argparse
import socket
import threading
import json
import time

"""
Data is in the format:
{
  ("<UUID-X>", "1699520400") : {"bitrate": 3500, "framrate": 24},
    ....
}
"""
LOCAL_STORE = {}

MAX_BYTES_READ = 1024 # 1024 bytes should be enough for UDP.
FRAME_RATE = 'framerate'
BIT_RATE = 'bitrate'
SLEEP_BETWEEN_WRITE_IN_SEC = 1.0

def socket_reader(port: int, rate_var: str, lock: threading.Lock):
    """
    Function reads infintely from the given port and writes data to LOCAL_STORE.
    Since both sockets are sending data in the same format with only a single difference,
    and handling is also similar, we are resuing this function. If different / subject to change,
    we would use 2 separate functions, atleast for parsing part.
    """
    # Socket is going to be bound to a UDP stream with a defined IP address.
    sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    # Socket is to be bound to a given local port.
    sock.bind(('0.0.0.0', port))

    try:
        # Stream is infinite
        while True:
            # Single entry read from socket
            msg = sock.recv(MAX_BYTES_READ) 
            if not msg:
                raise InterruptedError(f"Socket bound to port {port} ")
            # JSON here has all data in string format
            json_data = json.loads(msg)

            player = json_data['video_player']
            log_time = json_data['utc_minute']
            k = (player, log_time)

            # Lock is taken before the block and released at the end of the block.
            # Needed to prevent simultaneous read / write to same entry
            with lock:
                if k not in LOCAL_STORE:
                    LOCAL_STORE[k] = {}
            LOCAL_STORE[k][rate_var] = msg[rate_var]
    
    except Exception as err:
        print(f"Received Exception {err}, {type(err)}")
    finally:
        print(f"Exiting reading stream on port {port}")


def write_combined_data(lock: threading.Lock):
    while True:

        deletion_list = []
        for k, v in LOCAL_STORE.items():
            # Only if both streams have been processed, should we proceed.
            if BIT_RATE in v and FRAME_RATE in v:
                # Since we can't delete while processing, we keep a separate list for this.
                deletion_list.append(k)
                player, log_time = k
                print(f"video_player {player} is at bitrate {v[BIT_RATE]} and framerate {v[FRAME_RATE]} at {log_time}")

        # data lock is to be acquired since the data musn't be modifiable here.
        with lock:
            for d in deletion_list:
                del LOCAL_STORE[d]
        
        # Since we already processed all data present in local store, we sleep for s small time.
        time.sleep(SLEEP_BETWEEN_WRITE_IN_SEC)


def log_aggregator():
    parser = argparse.ArgumentParser(
        prog='Log Stream Aggregator',
        description='Reads 2 streams of data, aggregates them and '
        'writes the combined logs to CLI')
    parser.add_argument('-pA', '--portA', required=True, type=int,
                        help='Port of stream A where data is being published')
    parser.add_argument('-pB', '--portB', required=True, type=int,
                        help='Port of stream B where data is being published')
    args = parser.parse_args()

    portA = args['portA']
    portB = args['portB']
    data_lock = threading.Lock()


    socketA_reader = threading.Thread(target=socket_reader,
                                      args=(portA, BIT_RATE, data_lock),
                                      name='socketA',
                                      daemon=True)
    socketB_reader = threading.Thread(target=socket_reader,
                                      args=(portB, FRAME_RATE, data_lock),
                                      name='socketB',
                                      daemon=True)
    
    socketA_reader.run()
    socketB_reader.run()

    write_combined_data(data_lock)




if __name__ == "main":
    log_aggregator()
