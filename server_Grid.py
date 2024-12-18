import socket
import pickle
import threading
import time
import struct  # Para envio/recebimento do tamanho dos dados

HOST = '127.0.0.1'
PORT = 5000

results = []  # Lista para armazenar os subconjuntos ordenados
worker_times = {}  # Dicionário para armazenar os tempos individuais dos workers
worker_ips = []  # Lista de IPs dos workers conectados

# Função para ler a lista do arquivo
def ler_arquivo(nome_arquivo):
    try:
        with open(nome_arquivo, "r") as file:
            return [int(line.strip()) for line in file.readlines()]
    except FileNotFoundError:
        print("Arquivo não encontrado. Verifique o nome.")
        exit()

# Divide a lista em subconjuntos
def dividir_lista(lista, num_chunks):
    avg = len(lista) // num_chunks
    return [lista[i * avg: (i + 1) * avg] for i in range(num_chunks)]

# Recebe dados dinâmicos
def recv_data(conn):
    data_size = struct.unpack("!I", conn.recv(4))[0]
    data = b""
    while len(data) < data_size:
        packet = conn.recv(min(4096, data_size - len(data)))
        if not packet:
            raise ConnectionResetError("Conexão perdida.")
        data += packet
    return pickle.loads(data)

# Envia dados dinâmicos
def send_data(conn, obj):
    serialized_data = pickle.dumps(obj)
    conn.sendall(struct.pack("!I", len(serialized_data)))
    conn.sendall(serialized_data)

# Função para lidar com cada worker
def handle_worker(conn, addr, worker_id):
    print(f"[SERVIDOR] Conectado ao worker {addr}")
    worker_ips.append(addr[0])
    start_time = time.time()
    try:
        # Enviar subconjunto
        chunk = data_chunks.pop(0)
        send_data(conn, chunk)
        print(f"[SERVIDOR] Enviado subconjunto: {chunk}")

        # Receber subconjunto ordenado e tempo do worker
        sorted_chunk = recv_data(conn)
        worker_time = recv_data(conn)  # Tempo de execução do worker
        print(f"[SERVIDOR] Recebido ordenado: {sorted_chunk}")
        results.append(sorted_chunk)
        worker_times[worker_id] = worker_time
    finally:
        conn.close()
        print(f"[SERVIDOR] Worker {addr} finalizou em {time.time() - start_time:.2f} segundos")

if __name__ == "__main__":
    nome_arquivo = input("Digite o nome do arquivo com a lista de números: ")
    lista_completa = ler_arquivo(nome_arquivo)
    num_chunks = int(input("Digite o número de workers esperados: "))
    data_chunks = dividir_lista(lista_completa, num_chunks)

    print("[SERVIDOR] Subconjuntos preparados. Aguardando workers...")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        threads = []

        # Aguardar conexões
        for i in range(num_chunks):
            conn, addr = server_socket.accept()
            t = threading.Thread(target=handle_worker, args=(conn, addr, i + 1))
            threads.append(t)
            t.start()

        # Espera o comando do usuário para iniciar
        input("[SERVIDOR] Pressione ENTER para iniciar...")

        for t in threads:
            t.join()

        # Mesclar resultados
        resultado_final = sorted(sum(results, []))
        print(f"[SERVIDOR] Lista final ordenada: {resultado_final}")

        # Salvar resultado no arquivo
        resultado_arquivo = nome_arquivo.replace(".txt", "_resultado.txt")
        with open(resultado_arquivo, "w") as file:
            for num in resultado_final:
                file.write(f"{num}\n")
        print(f"[SERVIDOR] Resultado salvo em {resultado_arquivo}")

        # Calcular tempo total
        tempo_total = sum(worker_times.values())

        # Gerar log
        log_arquivo = nome_arquivo.replace(".txt", "_log.txt")
        with open(log_arquivo, "w") as log:
            log.write(f"Arquivo de entrada: {nome_arquivo}\n")
            log.write(f"Tamanho da lista de entrada: {len(lista_completa)}\n")
            log.write(f"Quantidade de workers: {num_chunks}\n")
            for worker_id, worker_time in worker_times.items():
                log.write(f"Worker {worker_id}: {worker_time:.2f} segundos\n")
            log.write(f"Tempo total: {tempo_total:.2f} segundos\n")
        print(f"[SERVIDOR] Log salvo em {log_arquivo}")
