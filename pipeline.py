# -*- coding: utf-8 -*-

import multiprocessing
from queue import Empty
from collections import deque
from collections.abc import Sequence, Mapping
import time

def stage(loop_func, queue_size=10, **kwargs):
    """ Decorator que transforma a função em um objeto da classe Stage. """
    return Stage(loop_func=loop_func, queue_size=queue_size, kwargs=kwargs)

class Stage(object):
    """ Classe que define um estágio do pipeline. """

    def __init__(self, loop_func=None, init_func=None, queue_size=10, args=(), kwargs={}):
        """ Construtor.
            Parâmetros:
            - func: Função a ser executada para cada novo dado de entrada do estágio.
            - queue_size: Tamanho da fila de saída.
            - args e kwargs: Parâmetros a serem passados para a função init_func.
        """
        self._queue_size = queue_size
        if init_func is not None:
            self.init = init_func
        if loop_func is not None:
            self.loop = loop_func
        self._input = None
        self._output = None
        self._queue = None
        self._event_stop = None
        self._process = None
        self._pipeline = None
        self._args = args
        self._kwargs = kwargs

    def __call__(self, stage=None):
        """ Conecta a entrada do estágio atual à saída do estágio fornecido como argumento,
            ou define o estágio atual como sendo o primeiro.
        """
        if stage is not None:
            self._input = stage
            stage._output = self
        self._event_stop = multiprocessing.Event()
        return self

    def init(self, *args, **kwargs):
        """ Método de inicialização do estágio, que pode ser redefinido em uma subclasse.
            Difere do método padrão __init__() por ser executado diretamente no processo filho.
        """
        pass

    def loop(self, *args, **kwargs):
        """ Método de loop do estágio, que pode ser redefinido em uma subclasse. """
        pass

    def _create_queue(self):
        """ Cria a fila de saída do estágio. """
        self._queue = multiprocessing.Queue(maxsize=self._queue_size)

    def _start(self):
        """ Inicia a execução do estágio do pipeline. """
        # Se for um pipeline com um único estágio
        if self._input is None and self._output is None:
            def target():
                self.init(*self._args, **self._kwargs)
                gen = self.loop()
                # Só termina se o sinal de parada for dado ou se o gerador chegar ao fim
                while not self._event_stop.is_set():
                    try:
                        next(gen)
                    except StopIteration:
                        self._event_stop.set()
        # Se for o primeiro estágio do pipeline
        elif self._input is None:
            self._create_queue()
            def target():
                self.init(*self._args, **self._kwargs)
                # Só termina se o sinal de parada for dado ou se o gerador chegar ao fim
                gen = self.loop()
                while not self._event_stop.is_set():
                    try:
                        output_data = next(gen)
                        self._queue.put(output_data, block=True, timeout=None)
                    except StopIteration:
                        self._event_stop.set()
                self._output._event_stop.set()
        # Se for o último estágio do pipeline
        elif self._output is None:
            def target():
                self.init(*self._args, **self._kwargs)
                # Só termina se o sinal de parada for dado e se a fila de entrada estiver vazia
                while not self._event_stop.is_set() or not self._input._queue.empty():
                    try:
                        input_data = self._input._queue.get(block=True, timeout=1)
                        self._call_loop_func(input_data)
                    except queue.Empty:
                        pass
        # Demais estágios do pipeline
        else:
            self._create_queue()
            def target():
                self.init(*self._args, **self._kwargs)
                # Só termina se o sinal de parada for dado e se a fila de entrada estiver vazia
                while not self._event_stop.is_set() or not self._input._queue.empty():
                    try:
                        input_data = self._input._queue.get(block=True, timeout=1)
                        output_data = self._call_loop_func(input_data)
                        if self._queue is not None:
                            self._queue.put(output_data, block=True, timeout=None)
                    except queue.Empty:
                        pass
                if self._output is not None:
                    self._output._event_stop.set()
        self._process = multiprocessing.Process(target=target, daemon=True)
        self._process.start()

    def _call_loop_func(self, input_data):
        """ Método auxiliar para chamar a função de loop de acordo com o tipo de entrada. """
        if isinstance(input_data, Sequence):
            output_data = self.loop(*input_data)
        elif isinstance(input_data, Mapping):
            output_data = self.loop(**input_data)
        else:
            output_data = self.loop(input_data)
        return output_data

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
        while stage is not None:
            self.stages.appendleft(stage)
            stage = stage._input

    def add(self, stage):
        """ Adiciona um estágio ao pipeline. """
        if len(self.stages) == 0:
            stage()
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
