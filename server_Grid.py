import socket
import pickle
import threading
import time
import struct

results = []
worker_times = {}
worker_ips = []
worker_connections = []

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception as e:
        print(f"Erro ao obter IP local: {e}")
        return "127.0.0.1"

def ler_arquivo(nome_arquivo):
    try:
        with open(nome_arquivo, "r") as file:
            return [int(line.strip()) for line in file.readlines()]
    except FileNotFoundError:
        print("Arquivo não encontrado.")
        exit()

def dividir_lista(lista, num_chunks):
    avg = len(lista) // num_chunks
    return [lista[i * avg: (i + 1) * avg] for i in range(num_chunks)]

def recv_data(conn):
    data_size = struct.unpack("!I", conn.recv(4))[0]
    data = b""
    while len(data) < data_size:
        packet = conn.recv(min(4096, data_size - len(data)))
        if not packet:
            raise ConnectionResetError("Conexão perdida.")
        data += packet
    return pickle.loads(data)

def send_data(conn, obj):
    serialized_data = pickle.dumps(obj)
    conn.sendall(struct.pack("!I", len(serialized_data)))
    conn.sendall(serialized_data)

def formatar_lista(lista):
    """Formata a lista para exibição compacta no terminal."""
    if len(lista) <= 5:
        return f"{lista}"
    tamanho = len(lista)
    return f"{{{lista[0]}, ..., {lista[tamanho//5]}, ..., {lista[2*tamanho//5]}, ..., {lista[3*tamanho//5]}, ..., {lista[4*tamanho//5]}, ..., {lista[-1]}}}"

def handle_worker(conn, addr, worker_id, chunk):
    print(f"[SERVIDOR] Worker {addr} iniciado.")
    worker_ips.append(addr[0])
    start_time = time.time()
    try:
        # Envia o subconjunto ao worker
        send_data(conn, "START")  # Sinal para iniciar
        print(f"[SERVIDOR] Enviado subconjunto: {formatar_lista(chunk)}")
        send_data(conn, chunk)

        # Recebe o subconjunto ordenado
        sorted_chunk = recv_data(conn)
        print(f"[SERVIDOR] Recebido ordenado: {formatar_lista(sorted_chunk)}")

        # Recebe o tempo de execução do worker
        worker_time = recv_data(conn)
        results.append(sorted_chunk)
        worker_times[worker_id] = worker_time
    finally:
        conn.close()

if __name__ == "__main__":
    local_ip = get_local_ip()
    PORT = 5000

    print(f"[SERVIDOR] Inicializando no IP: {local_ip}, Porta: {PORT}")
    nome_arquivo = input("Digite o nome do arquivo com a lista: ")
    lista_completa = ler_arquivo(nome_arquivo)
    print(f"[SERVIDOR] Lista original: {formatar_lista(lista_completa)}")

    num_chunks = int(input("Digite o número de workers esperados: "))
    data_chunks = dividir_lista(lista_completa, num_chunks)
    print(f"[SERVIDOR] Subconjuntos preparados: {[formatar_lista(chunk) for chunk in data_chunks]}")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((local_ip, PORT))
        server_socket.listen()
        print(f"[SERVIDOR] Aguardando conexões em {local_ip}:{PORT}...")

        # Aceitar conexões dos workers
        for i in range(num_chunks):
            conn, addr = server_socket.accept()
            worker_connections.append((conn, addr))
            print(f"[SERVIDOR] Worker conectado: {addr[0]}:{addr[1]}")

        # Esperar pelo comando do usuário
        input("[SERVIDOR] Pressione ENTER para iniciar a distribuição de tarefas...")

        # Criar threads para distribuir as tarefas
        threads = []
        for i, (conn, addr) in enumerate(worker_connections):
            t = threading.Thread(target=handle_worker, args=(conn, addr, i + 1, data_chunks[i]))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Mesclar os resultados finais
        resultado_final = sorted(sum(results, []))
        print(f"[SERVIDOR] Lista final ordenada: {formatar_lista(resultado_final)}")
