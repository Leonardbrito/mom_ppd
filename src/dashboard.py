"""Dashboard (bloco 6 do diagrama) — SUBSCRIBER do tópico de palavras.

Recebe as contagens publicadas pelos workers e atualiza, em tempo real, o
total de cada palavra-chave. Normalmente é aberto pelo cliente:

    python src/dashboard.py --run-id <id>            # interface gráfica (tkinter)
    python src/dashboard.py --run-id <id> --cli      # sem janela, imprime ao final

A fila `dashboard.<id>` já é criada (e ligada ao tópico) pelo cliente antes de
qualquer leitor começar, para que nenhuma contagem se perca.
"""
import argparse
import json
import queue
import threading
import time
from collections import Counter

import config


# --------------------------------------------------------------------------
# Estado agregado (independente de interface)
# --------------------------------------------------------------------------
class Estado:
    def __init__(self):
        self.arquivo = ""
        self.palavras = []
        self.totais = {}
        self.por_worker = Counter()
        self.processadas = 0
        self.esperado = None        # só conhecido quando os 2 leitores terminam
        self.leitores_fim = {}      # paridade -> linhas enviadas
        self.inicio = None          # instante da 1ª contagem
        self.fim = None
        self.iniciado = False

    def processar(self, chave, p):
        if chave == "inicio":
            self.arquivo = p["arquivo"]
            self.palavras = list(p["palavras"])
            self.totais = {w: 0 for w in self.palavras}
            self.iniciado = True
        elif chave == "contagem":
            if self.inicio is None:
                self.inicio = time.time()
            for palavra, n in p["contagem"].items():
                self.totais[palavra] = self.totais.get(palavra, 0) + n
            self.por_worker[p["worker"]] += 1
            self.processadas += 1
        elif chave == "leitor_fim":
            self.leitores_fim[p["paridade"]] = p["total"]
            if len(self.leitores_fim) == 2:
                self.esperado = sum(self.leitores_fim.values())
        if self.concluido and self.fim is None:
            self.fim = time.time()

    @property
    def concluido(self):
        return self.esperado is not None and self.processadas >= self.esperado

    @property
    def decorrido(self):
        if self.inicio is None:
            return 0.0
        return (self.fim or time.time()) - self.inicio

    @property
    def linhas_por_seg(self):
        d = self.decorrido
        return self.processadas / d if d > 0 else 0.0


# --------------------------------------------------------------------------
# Assinante: thread dona da conexão com o broker
# --------------------------------------------------------------------------
class Assinante(threading.Thread):
    """Consome a fila do dashboard e repassa (chave, payload) numa queue.Queue.

    O pika BlockingConnection não é thread-safe, então toda a comunicação com
    o broker fica nesta thread; a interface só lê a queue.Queue local.
    """

    def __init__(self, run_id, saida):
        super().__init__(daemon=True)
        self.run_id = run_id
        self.saida = saida
        self.conn = None
        self.canal = None
        self.erro = None

    def run(self):
        try:
            self.conn = config.conectar()
            self.canal = self.conn.channel()
            fila = config.nome_fila_dashboard(self.run_id)
            self.canal.basic_qos(prefetch_count=500)
            self.canal.basic_consume(queue=fila, on_message_callback=self._ao_receber)
            self.canal.start_consuming()
        except Exception as e:  # broker caiu, fila inexistente, etc.
            self.erro = str(e)
            self.saida.put(("erro", {"mensagem": self.erro}))
        finally:
            try:
                if self.conn and self.conn.is_open:
                    self.conn.close()
            except Exception:
                pass

    def _ao_receber(self, ch, metodo, props, corpo):
        chave = metodo.routing_key.split(".")[0]
        self.saida.put((chave, json.loads(corpo)))
        ch.basic_ack(delivery_tag=metodo.delivery_tag)

    def parar(self):
        """Encerra o consumo (chamável de outra thread) e apaga a fila."""
        def _fechar():
            try:
                self.canal.queue_delete(queue=config.nome_fila_dashboard(self.run_id))
            except Exception:
                pass
            self.canal.stop_consuming()
        try:
            self.conn.add_callback_threadsafe(_fechar)
        except Exception:
            pass


# --------------------------------------------------------------------------
# Modo texto (sem janela)
# --------------------------------------------------------------------------
def rodar_cli(run_id):
    fila = queue.Queue()
    estado = Estado()
    ass = Assinante(run_id, fila)
    ass.start()
    ultimo = 0
    while not estado.concluido:
        chave, payload = fila.get()
        if chave == "erro":
            raise SystemExit(f"Erro no assinante: {payload['mensagem']}")
        estado.processar(chave, payload)
        if estado.processadas - ultimo >= 25000:
            ultimo = estado.processadas
            print(f"... {estado.processadas} linhas processadas", flush=True)
    ass.parar()
    print(f"\nArquivo: {estado.arquivo}")
    print(f"Linhas: {estado.processadas}  |  {estado.decorrido:.2f}s  |  {estado.linhas_por_seg:.0f} linhas/s")
    for palavra in estado.palavras:
        print(f"  {palavra:<20} {estado.totais[palavra]}")
    print("Por worker:", dict(estado.por_worker))


# --------------------------------------------------------------------------
# Interface gráfica
# --------------------------------------------------------------------------
def rodar_gui(run_id):
    import tkinter as tk
    from tkinter import ttk

    fila = queue.Queue()
    estado = Estado()
    ass = Assinante(run_id, fila)
    ass.start()

    root = tk.Tk()
    root.title("Dashboard — Contagem de Palavras-Chave")
    root.geometry("720x620")
    root.minsize(560, 480)

    BG, CARD, FG, MUTED, ACC = "#f4f6fb", "#ffffff", "#1f2937", "#6b7280", "#3b6fd4"
    OK = "#1f9d55"
    root.configure(bg=BG)
    estilo = ttk.Style()
    estilo.theme_use("clam")
    estilo.configure("TProgressbar", troughcolor="#e5e7eb", background=ACC, thickness=14)

    topo = tk.Frame(root, bg=BG)
    topo.pack(fill="x", padx=18, pady=(16, 6))
    tk.Label(topo, text="Dashboard", bg=BG, fg=FG, font=("Helvetica", 18, "bold")).pack(anchor="w")
    lbl_arquivo = tk.Label(topo, text="Aguardando início...", bg=BG, fg=MUTED, font=("Helvetica", 10))
    lbl_arquivo.pack(anchor="w")

    # progresso
    prog = tk.Frame(root, bg=CARD, highlightbackground="#e5e7eb", highlightthickness=1)
    prog.pack(fill="x", padx=18, pady=6)
    lbl_status = tk.Label(prog, text="Aguardando leitores...", bg=CARD, fg=FG, font=("Helvetica", 11, "bold"))
    lbl_status.pack(anchor="w", padx=12, pady=(10, 2))
    barra = ttk.Progressbar(prog, mode="indeterminate", maximum=100)
    barra.pack(fill="x", padx=12, pady=4)
    barra.start(15)
    lbl_stats = tk.Label(prog, text="", bg=CARD, fg=MUTED, font=("Helvetica", 10))
    lbl_stats.pack(anchor="w", padx=12, pady=(2, 10))

    # tabela de palavras
    tk.Label(root, text="Ocorrências por palavra-chave", bg=BG, fg=FG,
             font=("Helvetica", 12, "bold")).pack(anchor="w", padx=18, pady=(10, 2))
    corpo = tk.Frame(root, bg=CARD, highlightbackground="#e5e7eb", highlightthickness=1)
    corpo.pack(fill="both", expand=True, padx=18, pady=4)
    linhas_ui = {}  # palavra -> (label_contagem, canvas)

    def montar_palavras():
        for w in corpo.winfo_children():
            w.destroy()
        linhas_ui.clear()
        corpo.columnconfigure(2, weight=1)
        for i, palavra in enumerate(estado.palavras):
            tk.Label(corpo, text=palavra, bg=CARD, fg=FG, font=("Helvetica", 11),
                     anchor="w", width=16).grid(row=i, column=0, sticky="w", padx=(12, 4), pady=5)
            lc = tk.Label(corpo, text="0", bg=CARD, fg=FG, font=("Helvetica", 11, "bold"),
                          anchor="e", width=9)
            lc.grid(row=i, column=1, padx=4)
            cv = tk.Canvas(corpo, height=16, bg="#eef1f6", highlightthickness=0)
            cv.grid(row=i, column=2, sticky="ew", padx=(4, 12))
            linhas_ui[palavra] = (lc, cv)

    # workers
    tk.Label(root, text="Linhas processadas por worker", bg=BG, fg=FG,
             font=("Helvetica", 12, "bold")).pack(anchor="w", padx=18, pady=(10, 2))
    lbl_workers = tk.Label(root, text="—", bg=CARD, fg=MUTED, font=("Courier", 10),
                           anchor="w", justify="left", highlightbackground="#e5e7eb",
                           highlightthickness=1, padx=12, pady=8)
    lbl_workers.pack(fill="x", padx=18, pady=(4, 16))

    def desenhar_barras():
        maior = max(estado.totais.values(), default=0) or 1
        for palavra, (lc, cv) in linhas_ui.items():
            n = estado.totais.get(palavra, 0)
            lc.config(text=f"{n:,}".replace(",", "."))
            cv.delete("all")
            largura = cv.winfo_width()
            cv.create_rectangle(0, 0, int(largura * n / maior), 16,
                                fill=OK if estado.concluido else ACC, width=0)

    def atualizar():
        sujo = False
        for _ in range(5000):  # limita o trabalho por ciclo para a janela não travar
            try:
                chave, payload = fila.get_nowait()
            except queue.Empty:
                break
            if chave == "erro":
                lbl_status.config(text=f"Erro de conexão: {payload['mensagem']}", fg="#c0392b")
                break
            era_iniciado = estado.iniciado
            estado.processar(chave, payload)
            if chave == "inicio" and not era_iniciado:
                lbl_arquivo.config(text=f"Arquivo: {estado.arquivo}")
                montar_palavras()
            sujo = True

        if sujo:
            desenhar_barras()
            lbl_workers.config(
                text="\n".join(f"{w:<12} {n:>10}" for w, n in sorted(estado.por_worker.items())) or "—")
            if estado.esperado is not None:
                barra.stop()
                barra.config(mode="determinate", maximum=max(estado.esperado, 1),
                             value=estado.processadas)
                lbl_stats.config(
                    text=f"{estado.processadas:,} de {estado.esperado:,} linhas  •  "
                         f"{estado.linhas_por_seg:,.0f} linhas/s  •  {estado.decorrido:.1f}s".replace(",", "."))
            else:
                lbl_stats.config(text=f"{estado.processadas:,} linhas processadas  •  "
                                      f"{estado.linhas_por_seg:,.0f} linhas/s".replace(",", "."))
                lbl_status.config(text="Processando... (leitores ainda lendo o arquivo)")
            if estado.esperado is not None and not estado.concluido:
                lbl_status.config(text="Processando...", fg=FG)
            if estado.concluido:
                lbl_status.config(text="✔ Concluído", fg=OK)
                ass.parar()
        root.after(100, atualizar)

    def ao_fechar():
        ass.parar()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", ao_fechar)
    root.after(100, atualizar)
    root.mainloop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Dashboard (subscriber)")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--cli", action="store_true", help="sem interface gráfica")
    a = ap.parse_args()
    (rodar_cli if a.cli else rodar_gui)(a.run_id)
