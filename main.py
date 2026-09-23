import time
import json
import threading
from multiprocessing import Process, Queue
from datetime import datetime

# api mockada
def consultar_api(id):
    time.sleep(0.001)  # tempo pra simular uma requisicao real

    resposta = {
        "id": id,
        "status": "ok",
        "valor": round((id * 3.14), 3)
    }

    return resposta


def processoThread(ids, arquivo_log, lock_log, lock_ids, indice):
    id_thread = threading.current_thread().name

    while True:

        with lock_ids:

            if indice[0] >= len(ids):
                break

            id = ids[indice[0]]
            indice[0] += 1

        agora = datetime.now()
        resposta_json = json.dumps(consultar_api(id))

        with lock_log:
            arquivo_log.write(
                f"{agora}, {id_thread}, {id}, {resposta_json}\n"
            )


# p1
def p1(NUM_THREADS, caminho_arquivo, caminho_log, fila_tempo):

    print(f"iniciando p1 com {NUM_THREADS} threads")

    inicio = time.perf_counter()

    lock_log = threading.Lock()
    lock_ids = threading.Lock()

    ids = []

    with open(caminho_arquivo, "r") as arquivo:
        for linha in arquivo:
            ids.append(int(linha.strip()))

    threads = []
    indice = [0]

    with open(caminho_log, "w", encoding="utf-8") as arquivo_log:

        for i in range(NUM_THREADS):

            thread = threading.Thread(
                target=processoThread,
                args=(ids, arquivo_log, lock_log, lock_ids, indice),
                name=f"Thread-{i+1}"
            )

            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    fim = time.perf_counter()

    tempo = fim - inicio

    fila_tempo.put(tempo)

    print("Log criado com sucesso!")
    print("encerrando p1")


def verificar_linhas(caminho_lista, caminho_log):

    with open(caminho_lista, "r", encoding="utf-8") as arquivo:
        qtd_ids = sum(1 for linha in arquivo)

    with open(caminho_log, "r", encoding="utf-8") as arquivo:
        qtd_logs = sum(1 for linha in arquivo)

    print(f"IDs na lista: {qtd_ids}")
    print(f"Linhas no log: {qtd_logs}")

    if qtd_ids == qtd_logs:
        return True
    else:
        return False


def executar_teste(num_threads, caminho_lista, caminho_log):

    fila_tempo = Queue()

    processo_filho = Process(
        target=p1,
        args=(num_threads, caminho_lista, caminho_log, fila_tempo)
    )

    processo_filho.start()
    processo_filho.join()

    tempo = fila_tempo.get()

    exitcode = processo_filho.exitcode

    if exitcode == 0:
        status_processo = "término normal"
    elif exitcode > 0:
        status_processo = "término com erro"
    else:
        status_processo = "término por sinal"

    log_completo = verificar_linhas(
        caminho_lista,
        caminho_log
    )

    if not log_completo:
        status = "enriquecimento incompleto"
    elif exitcode == 0:
        status = "OK"
    else:
        status = status_processo

    return tempo, status


# p0
def p0():

    print("iniciando p0")

    resultados = []

    testes = [
        (1, "listas/lista_ids_pequena.txt", "logs/pequena_1_thread.log"),
        (4, "listas/lista_ids_pequena.txt", "logs/pequena_4_threads.log"),

        (1, "listas/lista_ids_media.txt", "logs/media_1_thread.log"),
        (4, "listas/lista_ids_media.txt", "logs/media_4_threads.log"),

        (1, "listas/lista_ids_grande.txt", "logs/grande_1_thread.log"),
        (4, "listas/lista_ids_grande.txt", "logs/grande_4_threads.log")
    ]

    for num_threads, caminho_lista, caminho_log in testes:

        print()
        print("--------------------------------")
        print(f"Teste: {num_threads} threads")
        print(f"Lista: {caminho_lista}")
        print("--------------------------------")

        tempo, status = executar_teste(
            num_threads,
            caminho_lista,
            caminho_log
        )

        resultados.append({
            "threads": num_threads,
            "lista": caminho_lista,
            "tempo": tempo,
            "status": status
        })

    print()
    print("========== RESULTADOS ==========")

    for resultado in resultados:
        print(
            f"{resultado['threads']} threads | "
            f"{resultado['lista']} | "
            f"{resultado['tempo']:.4f}s | "
            f"{resultado['status']}"
        )

    print("encerrando p0")


# main
if __name__ == "__main__":
    p0()