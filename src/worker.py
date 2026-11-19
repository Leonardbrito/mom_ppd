"""Contador de Palavras (bloco 4 do diagrama) — CONSUMIDOR da fila + PUBLISHER do tópico.

Pode-se rodar quantas instâncias quiser: o RabbitMQ distribui as linhas entre
elas (round-robin), que é justamente o paralelismo do workflow.

Para cada linha: conta as palavras-chave, publica o resultado em
`contagem.<run>` e só então confirma (ack) a mensagem da fila. Se o worker
morrer no meio, a linha volta para a fila e outro worker a processa.
"""
import argparse
import json
import os
import sys

import config


def main():
    ap = argparse.ArgumentParser(description="Worker contador de palavras")
    ap.add_argument("--id", default=str(os.getpid()), help="identificador exibido no dashboard")
    ap.add_argument("--prefetch", type=int, default=100)
    args = ap.parse_args()
    nome = f"worker-{args.id}"

    conn = config.conectar()
    canal = conn.channel()
    config.declarar_base(canal)
    canal.basic_qos(prefetch_count=args.prefetch)

    cache_padroes = {}  # palavras (tupla) -> regexes compiladas

    def ao_receber(ch, metodo, props, corpo):
        msg = json.loads(corpo)
        chave = tuple(msg["palavras"])
        if chave not in cache_padroes:
            cache_padroes[chave] = config.compilar_padroes(chave)
        contagem = config.contar_palavras(msg["texto"], cache_padroes[chave])

        config.publicar_topico(ch, "contagem", msg["run_id"],
                               {"linha": msg["linha"], "contagem": contagem, "worker": nome})
        ch.basic_ack(delivery_tag=metodo.delivery_tag)

    canal.basic_consume(queue=config.FILA_LINHAS, on_message_callback=ao_receber)
    print(f"[{nome}] aguardando linhas...", flush=True)
    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        pass
    finally:
        if conn.is_open:
            conn.close()


if __name__ == "__main__":
    main()
    sys.exit(0)
