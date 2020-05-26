import threading
import concurrent.futures
import time
import uuid
import logging
import random
import queue

logging.basicConfig(level=logging.DEBUG, format='(%(threadName)-9s) %(message)s')
delay = lambda t: time.sleep(t)

portals = [('walmart', 5), ('heb', 1)]

# El sig diccionario definirá la configuracion de las colas
portals_queue = {
    portal: {
        'queue': queue.Queue(),
        'threads': [],
        'max_concurrency': max_concurrency,
    } 
    for portal, max_concurrency in portals
}


class ProducerThread(threading.Thread):
    """
    Esta clase simula ser el Productor de Jobs, intenta recrear el comportamiento de lo que sería hoy en día el watcher
    """
    def __init__(self, max_items_to_produce=None, args=(), kwargs=None, verbose=None):
        super(ProducerThread, self).__init__()
        self.max_items_to_produce = max_items_to_produce or 10

    def run(self):
        generated_items_count = 0
        while generated_items_count < self.max_items_to_produce:
            portal = random.choice([*portals_queue.keys()])
            item = str(uuid.uuid4())[:8]  # Se crea un hash cualquiera, es el parámetro que simula lo que se tiene que procesar
            portals_queue[portal]['queue'].put(item)
            generated_items_count += 1
            logging.debug(f"[{portal}] Se creo un job {item} (Hay {portals_queue[portal]['queue'].qsize()} items en la cola)")
                        
            # Al descomentar la sig linea entonces se producira y consumira al mismo tiempo (esta clase es un thread)
            # delay(random.uniform(1, 3))


def task(portal_message):
    """
    Esta tarea representa el request a b2b-scrapers
    """
    portal, message = portal_message
    log_prefix = f"[{portal}](PID: {threading.get_ident()})"
    logging.debug(f"{log_prefix} Requesting... {message}")
    
    delay(random.uniform(1, 30))  # Simulando el tiempo de ejecucion del request
    task_success = random.choice([true, false])  # TODO: Ver como tratar las tareas que fallan (task_success = False). Posiblemente reencolarlas(?)

    logging.debug(f"{log_prefix} Sucess: {task_success} for {message}")
    portals_queue[portal]['threads'].pop()  # Libero el slot

    return message


class ConsumerThread(threading.Thread):
    """
    Esta clase  
    """

    def __init__(self, portal=None, max_concurrency=10, args=(), kwargs=None, verbose=None):
        super(ConsumerThread,self).__init__()
        self.portal = portal
        self.max_concurrency = max_concurrency

    def run(self):
        log_prefix = f"[{self.portal}]"
        executor = concurrent.futures.ThreadPoolExecutor(max_workers = self.max_concurrency)
        while True:
            threads_qty = len(portals_queue[self.portal]['threads'])
            logging.debug(f"{log_prefix} Threads: {threads_qty} | Queue Size: {portals_queue[self.portal]['queue'].qsize()}")
            queue_is_empty = portals_queue[self.portal]['queue'].empty()

            if not queue_is_empty and threads_qty < self.max_concurrency:
                item = portals_queue[self.portal]['queue'].get()
                portals_queue[self.portal]['threads'].append(item)
                logging.debug(f"{log_prefix} Se obtuvo el trabajo: {item}")
                executor.map(task, [(self.portal, item)])
            elif not portals_queue[self.portal]['queue'].empty():
                logging.debug(f"{log_prefix} Esperando que se desocupen slots... Usados: {threads_qty}/{self.max_concurrency}")
            else:
                logging.debug(f"{log_prefix} Hay Slots disponibles... Esperando por más trabajo :)")
            
            delay(1)


if __name__ == '__main__':
    p = ProducerThread(max_items_to_produce=100)
    p.start()
    delay(2)

    # Constuir los consumidores
    for portal in portals_queue.keys():
        c = ConsumerThread(portal=portal, max_concurrency=portals_queue[portal]['max_concurrency'])
        c.start()
        delay(2)
