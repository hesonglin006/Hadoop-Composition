import socket

class Socket():
    @classmethod
    def send(cls, host, port, data):
        print "Connecting..." + str(host) + ":" + str(port) + "\n"
        # print "Data to be sent is: " + str(data)
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sk.connect((host, port))
            sk.sendall(data)
        except Exception, e:
            print "XXX Exception: ", e
        finally:
            sk.close()

    @classmethod
    def broadcast(cls, data):
        HOST = '<broadcast>'
        BROADCAST_PORT = 37006
        try:
            udpCliSock.sendto(str(data), (HOST, BROADCAST_PORT))
        except:
            # recreate the socket
            udpCliSock = socket.socket(AF_INET, SOCK_DGRAM)
            udpCliSock.bind(('', 0))
            udpCliSock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
            udpCliSock.sendto(str(data), (HOST, BROADCAST_PORT))
        finally:
            udpCliSock.close()