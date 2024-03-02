import socket
import struct


def send_ping_receive_pong():
    # Server address and port
    host = '127.0.0.1'
    port = 6663

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        while True:
            # Prepare "ping" message with length prefix
            message = b'ping'
            # Prefix message with its length
            prefixed_message = struct.pack('>I', len(message)) + message

            # Send message with length prefix
            sock.send(prefixed_message)
            # First, receive the length of the incoming response
            response_length = sock.recv(4)
            if response_length:
                response_length = struct.unpack('>I', response_length)[0]

                # Now receive the actual response of the specified length
                response = sock.recv(response_length)

                print(f"Received: {response.decode()}")


if __name__ == "__main__":
    send_ping_receive_pong()
