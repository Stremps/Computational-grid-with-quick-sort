import random

def gerar_lista_txt(tamanho, nome_arquivo):
    # Gera números aleatórios entre 1 e 1000
    numeros = [random.randint(1, 1000000) for _ in range(tamanho)]
    
    # Salva os números em um arquivo .txt
    with open(nome_arquivo, "w") as file:
        for numero in numeros:
            file.write(f"{numero}\n")
    print(f"Arquivo '{nome_arquivo}' gerado com {tamanho} elementos.")

if __name__ == "__main__":
    tamanho = int(input("Digite o tamanho da lista de números: "))
    nome_arquivo = input("Digite o nome do arquivo (ex: lista.txt): ")
    gerar_lista_txt(tamanho, nome_arquivo)