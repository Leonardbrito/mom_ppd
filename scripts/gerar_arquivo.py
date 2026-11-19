"""Gera um arquivo de texto grande para testar o workflow.

    python scripts/gerar_arquivo.py [nº de linhas] [saída]   (padrão: 200000, data/entrada.txt)

Imprime a contagem exata de algumas palavras para conferir com o dashboard.
"""
import os
import random
import re
import sys
from collections import Counter

PADRAO = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "entrada.txt")

VOCAB = ("python java erro sucesso fila topico broker mensagem worker leitor dashboard "
         "paralelo processo thread rede servidor cliente dados arquivo linha palavra "
         "contador sistema distribuido middleware evento").split()


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 200_000
    saida = sys.argv[2] if len(sys.argv) > 2 else PADRAO
    rnd = random.Random(42)
    cont = Counter()
    os.makedirs(os.path.dirname(os.path.abspath(saida)), exist_ok=True)
    with open(saida, "w", encoding="utf-8") as f:
        for _ in range(n):
            palavras = [rnd.choice(VOCAB) for _ in range(rnd.randint(5, 20))]
            f.write(" ".join(palavras).capitalize() + ".\n")
            cont.update(re.findall(r"\w+", " ".join(palavras).lower()))
    print(f"{n} linhas gravadas em {saida}")
    print("Contagem de referência:", {w: cont[w] for w in ("python", "java", "erro", "middleware")})


if __name__ == "__main__":
    main()
