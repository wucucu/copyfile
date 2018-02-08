import sys, logging
from queue import Queue
from multiprocessing.connection import Listener

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def send(conn, msg):
    conn.send(msg)
    logger.info('sent msg: {}'.format(msg))


if __name__ == '__main__':
    address = ('localhost', 6000)
    queue_size = 2
    listener = Listener(address)

    logger.info('start queue service with queue size {}, address {}:{}'.format(queue_size, address[0], address[1]))

    while True:

        q = Queue(queue_size)

        with listener.accept() as conn:
            logger.info('connection accepted from {}'.format(listener.last_accepted))

            while True:
                msg = conn.recv()
                logger.info('received msg: {}'.format(msg))
                msg_type = msg[0]

                if msg_type == 'close':
                    logger.info('connection closed {}'.format(listener.last_accepted))
                    break

                if msg_type == 'send':
                    if q.full():
                        send(conn, ('fail',))
                    else:
                        send(conn, ('succ',))
                        q.put(msg[1])

                if msg_type == 'get':
                    if q.empty():
                        send(conn, ('fail',))
                    else:
                        send(conn, ('succ', q.get()))