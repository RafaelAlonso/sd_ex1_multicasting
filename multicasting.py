import os
import socket
import string
import thread
import pickle
import sys
import signal

class Message:
  def __init__(self, node_id, message_id, type):
    self.id = 'message_' + str(node_id) + '_' + str(message_id)
    self.node_id = node_id
    self.type = type # can be 'message' or 'ack'
    self.acks = {0: False, 1: False, 2: False, 3: False}
    self.acks[node_id] = True

# =======================================================================

class Node:
  def __init__(self, id):
    self.id = int(id)
    self.message_id = 1
    self.queue = []
    self.clock = self.id + 1

    self.destinations = [0,1,2,3]
    self.destinations.remove(self.id)

  # recebe o message e atualiza a tabela
  def update_queue(self, message):
    self.queue.push(message)

    meu_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('localhost', 25000 + message.node_id)
    meu_socket.connect(server_address)

    # envia o message
    pacote_codificado = pickle.dumps(message)
    meu_socket.send(pacote_codificado)
    meu_socket.close()

  # cria message e envia para todos
  def send_message(self, message):
    message = Message(self.id, self.message_id)
    self.queue.append(message)

    # Manda message para os vizinhos
    for destination in self.destinations:
      # abre o socket para o destino
      meu_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      server_address = ('localhost', 25000 + destination)
      meu_socket.connect(server_address)

      # envia o message
      pacote_codificado = pickle.dumps(message)
      meu_socket.send(pacote_codificado)
      meu_socket.close()

# =======================================================================

# Definindo a thread que recebe pacotes
def thread_recebe():
    serverPort = int(sys.argv[1])
    # Criando o socket
    serverSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        # O socket fica ouvindo o meio
        serverSocket.bind(('',serverPort))
        serverSocket.listen(1)
        while True:
            # Aceita uma conexao
            connectionSocket, addr = serverSocket.accept()
            try:
                # Recebe os dados e os decodifica
                data = connectionSocket.recv(1024)
                message = pickle.loads(data)
                # Passa o message para o node
                node.update_queue(message)
            except Exception as e:
              print 'Erro ao receber:', e
    except Exception as e:
      print 'Erro ao abrir o socket:', e

# Definindo a thread que envia pacotes
def thread_processo():
    try:
        raw_input("Enviar mensagem ?")
        node.send_message('Processo ', node.id, 'enviou a mensagem ', node.clock)
    except Exception as e:
      print 'Erro ao enviar message: ', e

# =======================================================================

global node
node = Node(sys.argv[2])

# Main
def main():
    PORT = sys.argv[1]
    thread.start_new_thread(thread_processo, ())
    thread.start_new_thread(thread_recebe, ())
    signal.pause()

if __name__ == "__main__":
    sys.exit(main())
