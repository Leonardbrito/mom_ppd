# Projeto MOM — Contagem paralela de palavras-chave

Workflow de computação paralela com **MOM (RabbitMQ)** em Python: dois leitores de linha
(pares/ímpares) alimentam uma **fila**; N workers consomem a fila, contam as palavras-chave e
publicam o resultado num **tópico**; o dashboard assina o tópico e atualiza os totais em tempo real.

## Como rodar

```bash
# 1) broker (precisa do Docker)
docker compose up -d

# 2) dependências (uma vez)
python3 -m venv .venv && ./.venv/bin/pip install -r requirements.txt   # tkinter vem com o Python (Ubuntu: sudo apt install python3-tk)

# 3) arquivo de teste (opcional) e aplicação
./.venv/bin/python scripts/gerar_arquivo.py 200000 data/entrada.txt
./.venv/bin/python src/cliente.py            # interface gráfica
```

Sem interface (útil para testar): `python src/cliente.py --cli -a data/entrada.txt -p python,java,erro -w 3`.
Painel do broker: http://localhost:15672 (guest/guest). Outro host: variável `RABBITMQ_HOST`.

## Arquitetura (numeração do diagrama do enunciado)

| # | Componente | Arquivo | Papel no MOM |
|---|---|---|---|
| 1 | Cliente | `src/cliente.py` | pede arquivo/palavras, sobe workers, dashboard e os 2 leitores |
| 2 | Leitor de linhas (par e ímpar) | `src/leitor.py` | **Produtor** da fila |
| 3 | Fila de linhas | `fila_linhas` (RabbitMQ) | fila durável, entrega round-robin |
| 4 | Contador de palavras (workers) | `src/worker.py` | **Consumidor** da fila + **Publisher** do tópico |
| 5 | Tópico de palavras | exchange `topico_palavras` (tipo *topic*) | publish/subscribe |
| 6 | Dashboard | `src/dashboard.py` | **Subscriber** do tópico |

`src/config.py` concentra a conexão e a topologia; `scripts/gerar_arquivo.py` cria o arquivo de teste.

### Fluxo de mensagens

1. Cliente cria a fila `dashboard.<run>` já ligada ao tópico (`*.<run>`) e publica `inicio.<run>`
   (arquivo + palavras). Isso vem *antes* dos leitores porque o tópico não guarda mensagens:
   sem a fila pré-criada, o dashboard poderia perder as primeiras contagens.
2. Cada leitor lê o arquivo em streaming, e para cada linha da sua paridade publica
   `{run_id, linha, texto, palavras}` em `fila_linhas`. Linhas numeradas a partir de 1
   (ímpar = 1, 3, 5…; par = 2, 4, 6…).
3. Os workers (competing consumers) pegam linhas da fila, contam cada palavra-chave
   (palavra inteira, sem diferenciar maiúsculas/minúsculas), publicam `contagem.<run>` com
   `{linha, contagem, worker}` e só então dão **ack**. Se um worker morrer antes do ack, a
   linha volta para a fila e outro a processa (entrega *at-least-once*).
4. Ao terminar, cada leitor publica `leitor_fim.<run>` com quantas linhas enviou. O dashboard soma
   os dois totais e sabe que acabou quando `linhas processadas == linhas enviadas`.
5. O dashboard soma as contagens, mostra barras por palavra, progresso, linhas/s e quantas linhas
   cada worker processou (evidência do paralelismo).

### Decisões de projeto (para a explicação do código)

- **Por que fila e tópico?** A fila distribui *trabalho* (cada linha vai para **um** worker); o
  tópico distribui *informação* (todo assinante interessado recebe). É a diferença entre os dois
  modelos de MOM pedidos no enunciado.
- **Mensagens persistentes + fila durável** e **ack manual** garantem que nenhuma linha se perde.
- **`prefetch`=100** nos workers: cada um recebe um lote de linhas por vez, em vez de uma ida ao
  broker por linha.
- **Threads no dashboard:** o pika `BlockingConnection` não é thread-safe; só a thread assinante
  fala com o broker e entrega os dados à interface (tkinter) por uma `queue.Queue`.
- **Isolamento por `run_id`:** resultados de execuções antigas não se misturam com a atual.

### Limitações conhecidas

- Com entrega *at-least-once*, uma falha de worker entre publicar e dar ack pode contar uma linha
  duas vezes (raro; dedupe por número de linha exigiria guardar todos os números no dashboard).
- Palavras são comparadas literalmente: "ação" ≠ "acao".
