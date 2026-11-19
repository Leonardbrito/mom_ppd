"""Configuração e topologia compartilhadas por todos os processos.

Topologia no broker (RabbitMQ):

    Leitores ──(Produtor)──▶ [fila_linhas] ──(Consumidor)──▶ Workers
                                                               │ Publisher
                                                               ▼
    Dashboard ◀──(Subscriber)── [fila dashboard.<run>] ◀── [exchange topico_palavras]

* fila_linhas        : fila durável, round-robin entre os workers (competing consumers).
* topico_palavras    : exchange do tipo "topic". Routing keys usadas:
                         inicio.<run>      -> cliente avisa palavras-chave/arquivo
                         contagem.<run>    -> worker publica contagem de UMA linha
                         leitor_fim.<run>  -> leitor avisa quantas linhas enviou
* dashboard.<run>    : fila do assinante (dashboard), ligada em "*.<run>".
"""
import json
import os
import re

import pika

BROKER_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
BROKER_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
BROKER_USER = os.environ.get("RABBITMQ_USER", "guest")
BROKER_PASS = os.environ.get("RABBITMQ_PASS", "guest")

FILA_LINHAS = "fila_linhas"
TOPICO = "topico_palavras"


def conectar():
    """Abre uma conexão bloqueante com o broker."""
    params = pika.ConnectionParameters(
        host=BROKER_HOST,
        port=BROKER_PORT,
        credentials=pika.PlainCredentials(BROKER_USER, BROKER_PASS),
        heartbeat=60,
        blocked_connection_timeout=300,
    )
    return pika.BlockingConnection(params)


def declarar_base(canal):
    """Declara (idempotente) a fila de linhas e o tópico."""
    canal.queue_declare(queue=FILA_LINHAS, durable=True)
    canal.exchange_declare(exchange=TOPICO, exchange_type="topic", durable=True)


def nome_fila_dashboard(run_id):
    return f"dashboard.{run_id}"


def preparar_dashboard(canal, run_id):
    """Cria a fila do dashboard JÁ LIGADA ao tópico.

    O cliente chama isto antes de disparar os leitores: como o tópico não
    guarda mensagens, a fila precisa existir antes da primeira publicação,
    senão o dashboard perderia as primeiras contagens.
    """
    declarar_base(canal)
    fila = nome_fila_dashboard(run_id)
    # x-expires: o broker apaga a fila sozinho se o dashboard nunca aparecer.
    canal.queue_declare(queue=fila, durable=False, arguments={"x-expires": 3_600_000})
    canal.queue_bind(queue=fila, exchange=TOPICO, routing_key=f"*.{run_id}")
    return fila


def publicar_topico(canal, chave, run_id, payload):
    canal.basic_publish(
        exchange=TOPICO,
        routing_key=f"{chave}.{run_id}",
        body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )


def contar_palavras(texto, padroes):
    """Conta ocorrências de cada palavra-chave (inteira, sem diferenciar maiúsculas)."""
    return {palavra: len(p.findall(texto)) for palavra, p in padroes.items()}


def compilar_padroes(palavras):
    # (?<!\w) / (?!\w): borda de palavra que também funciona com "c++", "node.js"...
    return {
        p: re.compile(r"(?<!\w)" + re.escape(p) + r"(?!\w)", re.IGNORECASE | re.UNICODE)
        for p in palavras
    }
