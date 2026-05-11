import argparse
import random
import socket
import time


WORD_MESSAGES = [
    "spark streaming",
    "spark spark window",
    "driver brake warning",
    "stream data arrives",
]

BRAKE_MESSAGES = [
    "-1.2",
    "-3.8",
    "-5.1",
    "-6.0",
    "-2.4",
    "-7.3",
    "0.4",
]


def build_message(mode: str) -> str:
    if mode == "word":
        return random.choice(WORD_MESSAGES)
    return random.choice(BRAKE_MESSAGES)


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple TCP stream sender for Spark Streaming demos.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--mode", choices=["word", "brake"], default="word")
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((args.host, args.port))
        server.listen(1)
        print(f"Waiting for Spark Streaming to connect on {args.host}:{args.port} ...")
        conn, addr = server.accept()
        with conn:
            print(f"Connected by {addr}")
            while True:
                message = build_message(args.mode)
                conn.sendall((message + "\n").encode("utf-8"))
                print(f"sent: {message}")
                time.sleep(args.interval)


if __name__ == "__main__":
    main()
