"""Cliente (bloco 1 do diagrama) — ponto de partida da aplicação.

Pede o arquivo e as palavras-chave, e então:
  1. prepara a fila do dashboard no broker e anuncia o início (tópico `inicio.<run>`);
  2. garante os workers (contadores) em execução;
  3. abre o dashboard;
  4. instancia as DUAS execuções do leitor de linhas: uma para linhas pares e
     outra para ímpares.

Uso:
    python src/cliente.py                                   # interface gráfica
    python src/cliente.py --cli -a arquivo.txt -p python,java,erro -w 3
"""
import argparse
import os
import subprocess
import sys
import threading
import uuid

import config

DIR = os.path.dirname(os.path.abspath(__file__))


def _py(script, *args, **kw):
    return subprocess.Popen([sys.executable, "-u", os.path.join(DIR, script), *args], cwd=DIR, **kw)


class Orquestrador:
    """Lógica do cliente, independente de interface."""

    def __init__(self):
        self.workers = []
        self.leitores = []

    def _garantir_workers(self, n):
        self.workers = [w for w in self.workers if w.poll() is None]
        for i in range(len(self.workers), n):
            self.workers.append(_py("worker.py", "--id", str(i + 1),
                                    stdout=subprocess.DEVNULL))

    def iniciar(self, arquivo, palavras, n_workers, cli=False, ao_log=print):
        """Dispara a execução. Retorna (run_id, processo_do_dashboard)."""
        if not os.path.isfile(arquivo):
            raise ValueError(f"Arquivo não encontrado: {arquivo}")
        palavras = list(dict.fromkeys(p.strip() for p in palavras if p.strip()))
        if not palavras:
            raise ValueError("Informe ao menos uma palavra-chave.")

        run_id = uuid.uuid4().hex[:8]

        # 1) topologia + aviso de início (falha aqui = broker fora do ar)
        try:
            conn = config.conectar()
        except Exception as e:
            raise ConnectionError(
                f"Não foi possível conectar ao RabbitMQ em {config.BROKER_HOST}:{config.BROKER_PORT}.\n"
                f"Suba o broker com:  docker compose up -d\n({e})")
        canal = conn.channel()
        config.preparar_dashboard(canal, run_id)
        config.publicar_topico(canal, "inicio", run_id,
                               {"arquivo": os.path.abspath(arquivo), "palavras": palavras})
        conn.close()
        ao_log(f"Execução {run_id}: palavras = {', '.join(palavras)}")

        # 2) workers
        self._garantir_workers(n_workers)
        ao_log(f"{len(self.workers)} worker(s) ativo(s)")

        # 3) dashboard
        dash_args = ["--run-id", run_id] + (["--cli"] if cli else [])
        dash = _py("dashboard.py", *dash_args)
        ao_log("Dashboard aberto")

        # 4) dois leitores
        self.leitores = []
        for paridade in ("par", "impar"):
            p = _py("leitor.py", "--arquivo", os.path.abspath(arquivo), "--paridade", paridade,
                    "--run-id", run_id, "--palavras", ",".join(palavras),
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            self.leitores.append(p)
            threading.Thread(target=self._repassar, args=(p, ao_log), daemon=True).start()
        ao_log("Leitores de linhas pares e ímpares iniciados")
        return run_id, dash

    @staticmethod
    def _repassar(proc, ao_log):
        for linha in proc.stdout:
            ao_log(linha.rstrip())

    def encerrar_workers(self):
        for w in self.workers:
            if w.poll() is None:
                w.terminate()
        self.workers = []


# --------------------------------------------------------------------------
def rodar_cli(a):
    orq = Orquestrador()
    try:
        _, dash = orq.iniciar(a.arquivo, a.palavras.split(","), a.workers, cli=True)
        dash.wait()
    finally:
        orq.encerrar_workers()


def rodar_gui():
    import queue
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk

    orq = Orquestrador()
    logs = queue.Queue()

    root = tk.Tk()
    root.title("Cliente — Contador de Palavras-Chave (MOM)")
    root.geometry("640x520")
    root.minsize(520, 440)
    BG, CARD, FG, MUTED, ACC = "#f4f6fb", "#ffffff", "#1f2937", "#6b7280", "#3b6fd4"
    root.configure(bg=BG)
    ttk.Style().theme_use("clam")

    tk.Label(root, text="Contador de Palavras-Chave", bg=BG, fg=FG,
             font=("Helvetica", 18, "bold")).pack(anchor="w", padx=20, pady=(18, 0))
    tk.Label(root, text="Workflow paralelo com RabbitMQ: leitores → fila → workers → tópico → dashboard",
             bg=BG, fg=MUTED, font=("Helvetica", 10)).pack(anchor="w", padx=20, pady=(0, 12))

    form = tk.Frame(root, bg=CARD, highlightbackground="#e5e7eb", highlightthickness=1)
    form.pack(fill="x", padx=20)
    form.columnconfigure(1, weight=1)

    def rotulo(texto, linha):
        tk.Label(form, text=texto, bg=CARD, fg=FG, font=("Helvetica", 10, "bold")).grid(
            row=linha, column=0, sticky="w", padx=14, pady=(12, 2), columnspan=3)

    rotulo("Arquivo de entrada", 0)
    var_arq = tk.StringVar()
    ttk.Entry(form, textvariable=var_arq).grid(row=1, column=0, columnspan=2, sticky="ew",
                                               padx=(14, 6), pady=2)

    def escolher():
        c = filedialog.askopenfilename(title="Escolha o arquivo de texto",
                                       filetypes=[("Texto", "*.txt *.log *.csv"), ("Todos", "*.*")])
        if c:
            var_arq.set(c)

    ttk.Button(form, text="Procurar…", command=escolher).grid(row=1, column=2, padx=(0, 14))

    rotulo("Palavras-chave (separadas por vírgula)", 2)
    var_pal = tk.StringVar()
    ttk.Entry(form, textvariable=var_pal).grid(row=3, column=0, columnspan=3, sticky="ew",
                                               padx=14, pady=2)

    rotulo("Workers (contadores em paralelo)", 4)
    var_w = tk.IntVar(value=3)
    ttk.Spinbox(form, from_=1, to=16, textvariable=var_w, width=6).grid(
        row=5, column=0, sticky="w", padx=14, pady=(2, 14))

    btn = tk.Button(root, text="▶  Iniciar contagem", bg=ACC, fg="white", relief="flat",
                    activebackground="#2f5bb5", activeforeground="white",
                    font=("Helvetica", 12, "bold"), pady=8, cursor="hand2")
    btn.pack(fill="x", padx=20, pady=14)

    tk.Label(root, text="Log", bg=BG, fg=FG, font=("Helvetica", 10, "bold")).pack(anchor="w", padx=20)
    txt = tk.Text(root, height=8, bg=CARD, fg=FG, font=("Courier", 9), relief="flat",
                  highlightbackground="#e5e7eb", highlightthickness=1, state="disabled")
    txt.pack(fill="both", expand=True, padx=20, pady=(2, 18))

    def escrever(msg):
        txt.config(state="normal")
        txt.insert("end", msg + "\n")
        txt.see("end")
        txt.config(state="disabled")

    def iniciar():
        palavras = var_pal.get().split(",")
        try:
            orq.iniciar(var_arq.get().strip(), palavras, var_w.get(), ao_log=logs.put)
        except (ValueError, ConnectionError) as e:
            messagebox.showerror("Não foi possível iniciar", str(e))

    btn.config(command=iniciar)

    def drenar():
        while True:
            try:
                escrever(logs.get_nowait())
            except queue.Empty:
                break
        root.after(150, drenar)

    def fechar():
        orq.encerrar_workers()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", fechar)
    root.after(150, drenar)
    root.mainloop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Cliente do workflow MOM")
    ap.add_argument("--cli", action="store_true", help="sem interface gráfica")
    ap.add_argument("-a", "--arquivo")
    ap.add_argument("-p", "--palavras", help="palavras separadas por vírgula")
    ap.add_argument("-w", "--workers", type=int, default=3)
    a = ap.parse_args()
    if a.cli:
        if not (a.arquivo and a.palavras):
            ap.error("--cli exige -a/--arquivo e -p/--palavras")
        rodar_cli(a)
    else:
        rodar_gui()
