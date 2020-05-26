import threading
import concurrent.futures
import time
import logging
import random
import queue

logging.basicConfig(level=logging.DEBUG, format='(%(threadName)-9s) %(message)s')

q = queue.Queue()
threads = []


class ProducerThread(threading.Thread):
    def __init__(self, max_items_to_produce=None, args=(), kwargs=None, verbose=None):
        super(ProducerThread, self).__init__()
        self.max_items_to_produce = max_items_to_produce or 10

    def run(self):
        generated_items_count = 0
        while generated_items_count < self.max_items_to_produce:
            item = random.randint(1,10)
            q.put(item)
            generated_items_count += 1
            logging.debug(f'Se agrego el trabajo: {item} (Hay {q.qsize()} items en la cola)')
            # time.sleep(random.uniform(1, 3))


def task(message):
    """
    Esta tarea representa el request a b2b-scrapers
    """
    logging.debug(f"Se realizará request para {message} (Estoy en el pid: {threading.get_ident()})")
    time.sleep(random.uniform(1, 30))
    logging.debug(f"Request listo {message} (Estoy en el pid: {threading.get_ident()})")
    threads.pop()
    return message


class ConsumerThread(threading.Thread):
    MAX_CONCURRENCY = 10

    def __init__(self, name=None, args=(), kwargs=None, verbose=None):
        super(ConsumerThread,self).__init__()
        self.name = name

    def run(self):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers = self.MAX_CONCURRENCY)
        while True:
            logging.debug(f"Threads: {len(threads)} | Queue Size: {q.qsize()}")
            if not q.empty() and len(threads) < self.MAX_CONCURRENCY:
                item = q.get()
                threads.append(item)
                logging.debug(f'Se obtuvo el trabajo: {item}')
                executor.map(task, [item])
            elif not q.empty():
                logging.debug(f"Esperando que se desocupen slots... Usados: {len(threads)}/{self.MAX_CONCURRENCY}")
            else:
                logging.debug(f"Hay Slots disponibles... Esperando por más trabajo :)")
            
            time.sleep(1)

if __name__ == '__main__':
    p = ProducerThread(max_items_to_produce=100)
    c = ConsumerThread(name='walmart-consumer')

    p.start()
    time.sleep(2)
    c.start()
    time.sleep(2)
