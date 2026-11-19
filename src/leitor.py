"""Leitor de Linhas (bloco 2 do diagrama) — PRODUTOR da fila de linhas.

Lê o arquivo em streaming (sem carregar tudo na memória) e publica em
`fila_linhas` apenas as linhas da sua paridade. Numeração começa em 1:
  --paridade par   -> linhas 2, 4, 6, ...
  --paridade impar -> linhas 1, 3, 5, ...

Ao terminar, publica no tópico `leitor_fim.<run>` quantas linhas enviou; o
dashboard usa isso para saber o total e detectar o fim do processamento.
"""
import argparse
import json
import sys

import pika

import config


def main():
    ap = argparse.ArgumentParser(description="Leitor de linhas pares/ímpares")
    ap.add_argument("--arquivo", required=True)
    ap.add_argument("--paridade", required=True, choices=["par", "impar"])
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--palavras", required=True, help="palavras-chave separadas por vírgula")
    args = ap.parse_args()

    palavras = [p.strip() for p in args.palavras.split(",") if p.strip()]
    resto = 0 if args.paridade == "par" else 1

    conn = config.conectar()
    canal = conn.channel()
    config.declarar_base(canal)

    propriedades = pika.BasicProperties(delivery_mode=2)  # mensagem persistente
    enviadas = 0
    print(f"[leitor {args.paridade}] iniciado: {args.arquivo}", flush=True)

    with open(args.arquivo, "r", encoding="utf-8", errors="replace") as f:
        for numero, linha in enumerate(f, start=1):
            if numero % 2 != resto:
                continue
            corpo = json.dumps(
                {"run_id": args.run_id, "linha": numero, "texto": linha.rstrip("\n"),
                 "palavras": palavras},
                ensure_ascii=False,
            ).encode("utf-8")
            canal.basic_publish(exchange="", routing_key=config.FILA_LINHAS,
                                body=corpo, properties=propriedades)
            enviadas += 1

    config.publicar_topico(canal, "leitor_fim", args.run_id,
                           {"paridade": args.paridade, "total": enviadas})
    conn.close()
    print(f"[leitor {args.paridade}] terminou: {enviadas} linhas enviadas", flush=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
