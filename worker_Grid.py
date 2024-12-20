import socket
import pickle
import struct
import time

def quick_sort(arr):
    if len(arr) <= 1:
        return arr
    pivot = arr[len(arr) // 2]
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]
    return quick_sort(left) + middle + quick_sort(right)

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

if __name__ == "__main__":
    print("=== WORKER - Configuração de Conexão ===")
    host = input("Digite o IP do servidor [default: 127.0.0.1]: ").strip() or "127.0.0.1"
    port = int(input("Digite a porta do servidor [default: 5000]: ").strip() or 5000)

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((host, port))
            print("[WORKER] Conectado ao servidor, aguardando início...")

            # Aguardar o sinal do servidor
            start_signal = recv_data(client_socket)
            if start_signal == "START":
                print("[WORKER] Sinal de início recebido, processando dados...")

                data = recv_data(client_socket)
                print(f"[WORKER] Subconjunto recebido: {formatar_lista(data)}")
                
                start_time = time.time()
                sorted_data = quick_sort(data)
                end_time = time.time()
                elapsed_time = end_time - start_time

                print(f"[WORKER] Subconjunto ordenado: {formatar_lista(sorted_data)}")
                send_data(client_socket, sorted_data)
                send_data(client_socket, elapsed_time)
                print(f"[WORKER] Resultado enviado ao servidor ({elapsed_time:.2f} segundos).")
    except Exception as e:
        print(f"[WORKER] Erro: {e}")
