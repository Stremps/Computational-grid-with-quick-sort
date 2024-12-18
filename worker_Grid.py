import socket
import pickle
import struct
import time

# Quick Sort
def quick_sort(arr):
    if len(arr) <= 1:
        return arr
    pivot = arr[len(arr) // 2]
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]
    return quick_sort(left) + middle + quick_sort(right)

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

if __name__ == "__main__":
    print("=== WORKER - Configuração de Conexão ===")
    host = input("Digite o IP do servidor [default: 127.0.0.1]: ").strip() or "127.0.0.1"
    port = int(input("Digite a porta do servidor [default: 5000]: ").strip() or 5000)

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((host, port))
            print("[WORKER] Conectado ao servidor...")

            # Receber subconjunto
            data = recv_data(client_socket)
            print(f"[WORKER] Subconjunto recebido: {data}")
            
            # Ordenar e calcular tempo
            start_time = time.time()
            sorted_data = quick_sort(data)
            end_time = time.time()
            elapsed_time = end_time - start_time

            print(f"[WORKER] Subconjunto ordenado: {sorted_data}")
            send_data(client_socket, sorted_data)  # Enviar subconjunto ordenado
            send_data(client_socket, elapsed_time)  # Enviar tempo de execução

            print(f"[WORKER] Resultado e tempo enviados ao servidor ({elapsed_time:.2f} segundos).")
    except Exception as e:
        print(f"[WORKER] Erro: {e}")
