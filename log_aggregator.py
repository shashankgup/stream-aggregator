
import argparse
import socket
import threading
import json
import time

"""
Data is in the format:
{
  (<video_player>, <utc_minute>) : {"bitrate": <bitrate>, "framerate": <framerate>},
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

            if (rate_var not in json_data or 'video_player' not in json_data or 
                    'utc_minute' not in json_data):
                # This is an erroneous message in the stream. Skipping.
                continue

            player = json_data['video_player']
            log_time = json_data['utc_minute']
            k = (player, log_time)

            added = False

            if k not in LOCAL_STORE:
                # Lock is taken before the block and released at the end of the block.
                # Lock needed to prevent simultaneous write to same entry
                with lock:
                    # Double check locking to prevent race condition
                    if k not in LOCAL_STORE:
                        added = True
                        LOCAL_STORE[k] = {}
                        LOCAL_STORE[k][rate_var] = json_data[rate_var]

            # k exists already
            if not added:
                # Since other entry already exists in LOCAL_STORE, lock isn't needed.
                v = LOCAL_STORE[k]
                player, log_time = k

                # Get other rate from LOCAL_STORE
                bitrate, framerate = v.get(BIT_RATE), json_data[rate_var]
                if rate_var == BIT_RATE:
                    bitrate, framerate = json_data[rate_var], v[FRAME_RATE]
                
                print(f"video_player {player} is at bitrate {bitrate} and "
                        f"framerate {framerate} at {log_time}")
                # This value isn't needed any longer and must be deleted.
                del LOCAL_STORE[k]
    
    except Exception as err:
        print(f"Received Exception {err}, {type(err)}")
    finally:
        print(f"Exiting reading stream on port {port}")


def log_aggregator():
    parser = argparse.ArgumentParser(
        prog='Log Stream Aggregator',
        description='Reads 2 streams of data, aggregates them and '
        'writes the combined logs to CLI')
    parser.add_argument('-a', '--portA', required=True, type=int,
                        help='Port of stream A where data is being published')
    parser.add_argument('-b', '--portB', required=True, type=int,
                        help='Port of stream B where data is being published')
    args = parser.parse_args()

    portA = args.portA
    portB = args.portB
    data_lock = threading.Lock()


    socketA_reader = threading.Thread(target=socket_reader,
                                      args=(portA, BIT_RATE, data_lock),
                                      name='socketA',
                                      daemon=True)
    socketB_reader = threading.Thread(target=socket_reader,
                                      args=(portB, FRAME_RATE, data_lock),
                                      name='socketB',
                                      daemon=True)
    
    socketA_reader.start()
    socketB_reader.start()

    
    try:
        # Wait for both the daemon's to end, which should never happen.
        socketA_reader.join()
        socketB_reader.join()
    except KeyboardInterrupt:
        print("Received termination from CLI. Exiting now.")



if __name__ == "__main__":
    log_aggregator()
