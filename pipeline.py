# -*- coding: utf-8 -*-

import queue
import multiprocessing
from collections import deque
from collections.abc import Sequence, Mapping
import time

def stage(func, queue_size=10):
    return Stage(func, queue_size)

class Stage(object):
    """ Classe que define um estágio do pipeline. """

    def __init__(self, func, queue_size=10):
        """ Construtor.
            Parâmetros:
            - func: Função a ser executada para cada novo dado de entrada do estágio.
            - queue_size: Tamanho da fila de saída.
        """
        self._queue_size = queue_size
        self._func = func
        self._loop_func = None
        self._input = None
        self._output = None
        self._queue = None
        self._event_stop = None
        self._process = None
        self._pipeline = None

    def __call__(self, stage=None):
        """ Conecta a entrada do estágio atual à saída do estágio fornecido como argumento, ou
            ou define o estágio atual como sendo o primeiro.
        """
        if stage is not None:
            self._input = stage
            stage._output = self
        self._event_stop = multiprocessing.Event()
        return self

    def _create_queue(self):
        """ Cria a fila de saída do estágio. """
        self._queue = multiprocessing.Queue(maxsize=self._queue_size)

    def _start(self):
        """ Inicia a execução do estágio do pipeline. """
        # Se for o um pipeline com um único estágio
        if self._input is None and self._output is None:
            def loop_func():
                # Loop principal do estágio
                # Só termina se o sinal de parada for dado ou se o gerador chegar ao fim
                gen = self._func()
                while not self._event_stop.is_set():
                    try:
                        next(gen)
                    except StopIteration:
                        self._event_stop.set()
        # Se for o primeiro estágio do pipeline
        elif self._input is None:
            self._create_queue()
            def loop_func():
                # Loop principal do estágio
                # Só termina se o sinal de parada for dado ou se o gerador chegar ao fim
                gen = self._func()
                while not self._event_stop.is_set():
                    try:
                        output_data = next(gen)
                        self._queue.put(output_data, block=True, timeout=None)
                    except StopIteration:
                        self._event_stop.set()
                self._output._event_stop.set()
        # Se for o último estágio do pipeline
        elif self._output is None:
            def loop_func():
                # Loop principal do estágio
                # Só termina se o sinal de parada for dado e se a fila de entrada estiver vazia
                while not self._event_stop.is_set() or not self._input._queue.empty():
                    try:
                        input_data = self._input._queue.get(block=True, timeout=1)
                        if isinstance(input_data, Sequence):
                            output_data = self._func(*input_data)
                        elif isinstance(input_data, Mapping):
                            output_data = self._func(**input_data)
                        else:
                            output_data = self._func(input_data)
                    except queue.Empty:
                        pass
        # Demais estágios do pipeline
        else:
            self._create_queue()
            def loop_func():
                # Loop principal do estágio
                # Só termina se o sinal de parada for dado e se a fila de entrada estiver vazia
                while not self._event_stop.is_set() or not self._input._queue.empty():
                    try:
                        input_data = self._input._queue.get(block=True, timeout=1)
                        if isinstance(input_data, Sequence):
                            output_data = self._func(*input_data)
                        elif isinstance(input_data, Mapping):
                            output_data = self._func(**input_data)
                        else:
                            output_data = self._func(input_data)
                        if self._queue is not None:
                            self._queue.put(output_data, block=True, timeout=None)
                    except queue.Empty:
                        pass
                if self._output is not None:
                    self._output._event_stop.set()
        self._loop_func = loop_func
        self._process = multiprocessing.Process(target=self._loop_func, daemon=True)
        self._process.start()

    def _stop(self, timeout=4):
        """ Para a execução do estágio, esperando a conclusão da iteração atual. """
        self._event_stop.set()
        self._process.join(timeout)
        return self._process.exitcode

    def _force_stop(self):
        """ Força a parada imediata do estágio. """
        self._process.terminate()
            

class Pipeline(object):
    """ Classe que define um pipeline completo. """

    def __init__(self, last_stage=None):
        """ Construtor. """
        self.stages = deque()
        stage = last_stage
        while True:
            self.stages.appendleft(stage)
            if stage._input is None:
                stage._pipeline = self
                break
            stage = stage._input

    def add(self, stage):
        """ Adiciona um estágio ao pipeline. """
        if len(self.stages) == 0:
            stage()
            stage._pipeline = self
        else:
            stage(self.stages[-1])
        self.stages.append(stage)

    def start(self):
        """ Inicia a execução do pipeline. """
        for stage in self.stages:
            stage._start()

    def stop(self, timeout=16):
        """ Para a execução do pipeline, esperando o esvaziamento de todas as filas. """
        self.stages
        t0 = time.time()
        for stage in self.stages:
            dt = time.time() - t0
            exitcode = stage._stop(timeout-dt)
            if exitcode is None:
                stage._force_stop()

    def join(self, timeout=None):
        """ Espera o fim da execução do pipeline. """
        if timeout is None:
            for stage in self.stages:
                stage._process.join()
        else:
            t0 = time.time()
            for stage in self.stages:
                dt = time.time() - t0
                stage._process.join(timeout-dt)

    def force_stop(self):
        """ Força a parada imediata de todo o pipeline. """
        for stage in self.stages:
            stage._force_stop()
