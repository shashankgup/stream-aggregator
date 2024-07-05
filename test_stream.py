import socket
import json
import random

portA = 8080
portB = 9090

VIDEO_PLAYERS = ["<UUID-X>", "<UUID-Y>"]
TIME = [1000000, 2000000, 300000]

 # Length is 2(num_video_players) * 3(num_timestamps) = 6
FRAME_RATES = [13, 35, 27, 38, 84, 24]
BIT_RATES = [1302, 3531, 2764, 3883, 8438, 2484]


def send_message(port: int, json_data: dict):
    sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    msg = json.dumps(json_data).encode()
    sock.sendto(msg, ('0.0.0.0', port))

def get_random_for_A(stream_a):
    # Randomize send order
    i = random.randrange(0, len(stream_a))
    id = stream_a[i]
    vp_id = id % 2 # since 2 players exist
    time_id = id % 3 # since 3 times exist
    # removed processed i from STREAM
    stream_a = stream_a[0:i] + stream_a[i+1:]
    return stream_a, {
        "video_player": VIDEO_PLAYERS[vp_id],
        "utc_minute": TIME[time_id], 
        "bitrate": BIT_RATES[id]
        }
    
def get_random_for_B(stream_b):
    # Randomize send order
    i = random.randrange(0, len(stream_b))
    id = stream_b[i]
    vp_id = id % 2 # since 2 players exist
    time_id = id % 3 # since 3 times exist
    # removed processed i from STREAM
    stream_b = stream_b[0:i] + stream_b[i+1:]
    return stream_b, {
        "video_player": VIDEO_PLAYERS[vp_id],
        "utc_minute": TIME[time_id], 
        "framerate": FRAME_RATES[id]
    }

def send_all():
    stream_a = list(range(6))
    stream_b = list(range(6))
    while len(stream_a) > 0:
        stream_a, messageA = get_random_for_A(stream_a)
        send_message(portA, messageA)
        print(f"Sent to port {portA}: {messageA}")
        stream_b, messageB = get_random_for_B(stream_b)
        send_message(portB, messageB)
        print(f"Sent to port {portB}: {messageB}")

if __name__ == "__main__":
    send_all()
