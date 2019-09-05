# =======================================================================
# Guilherme Nishioka - RA 620246
# Rafael P. Alonso   - RA 620084
# =======================================================================

import os
import socket
import string
import thread
import pickle
import sys
import signal
import time
from random import randrange

def get_clock(elem):
  return elem.clock

class Message:
  def __init__(self, node_id, message_id, type, **args):
    self.id = message_id # the one that will identify every message
    self.node_id = node_id # the id of the sender
    self.type = type # can be 'message' or 'ack'
    self.clock = args.get('clock', None)

    # to keep track of the acks received...
    if (type == 'message'):
      self.acks = {0: False, 1: False, 2: False, 3: False}
      self.acks[node_id] = True

# =======================================================================

class Node:
  def __init__(self, id):
    self.id = int(id) # node id
    self.message_clock = int(id)
    self.queue = [] # where the messages will be stored
    self.acks = [] # where acks received before the message will be stored

    self.destinations = [0,1,2,3] # id of all the other nodes (to send the messages)
    self.destinations.remove(self.id) # removing the self.id (we're not sending anything to the node itself)

  def update_queue(self, message):
    # if the node received a message with the type 'message'...
    if (message.type == 'message'):
      print '\nnode' + str(self.id) + ' recebeu mensagem ' + message.id

      # print the current acks received before the actual messages, if any
      if self.acks:
        print '\nacks armazenados do node:'
        for ack in self.acks:
          print "- " + ack.id + ' / ' + str(ack.node_id)

      # set the ack to true and
      message.acks[self.id] = True

      # if any acks where received before this message, set them to true
      for ack in self.acks:
        if ack.id == message.id:
          print 'Ack vindo de ' + ack.node_id + ' encontrado!'
          message.acks[ack.node_id] = True
          self.acks.remove(ack)

      # set the clock
      self.message_clock = max(self.message_clock, self.queue[-1].clock)

      # append the message to the queue
      self.queue.append(message)

      # order the queue based on the message clock
      self.queue.sort(key=get_clock)

      # print the queue
      if self.queue:
        print '\nfila do node:'
        for msg in self.queue:
          print "- " + msg.id + ' ' + str(msg.acks.values()) + ', clock: ' + str(msg.clock)

      # create an 'ack' message to send to all the other nodes
      ack = Message(self.id, message.id, 'ack')

      # send the ack to all destinations
      thread.start_new_thread(thread_acks, (ack, self.destinations))

    else:
      # if the node received a message with the type 'ack'...
      print '\nnode' + str(self.id) + ' recebeu ack da mensagem ' + message.id + ', vindo do node' + str(message.node_id)


      if not filter(lambda msg: msg.id == message.id, self.queue):
        # if the message is not in the queue, just store it for later
        print '\nnode' + str(self.id) + ' nao recebeu a mensagem ' + message.id + ', entao esta guardando o ack.\n'
        self.acks.append(message)
      else:
        # get the message related to 'be acked' from the queue
        acked_message = filter(lambda msg: msg.id == message.id, self.queue)[0]

        # set the ack of the node that sent this message to True
        acked_message.acks[message.node_id] = True

        # check the message acks
        print 'acks da mensagem:'
        print acked_message.acks.values()

        # if all acks are True...
        if (all(ack == True for ack in acked_message.acks.values())):
          print 'Todos os acks foram recebidos. Removendo mensagem da fila...'

          # remove the message from the queue
          self.queue.remove(acked_message)

          # print the current queue
          if self.queue:
            print '\nfila do node'
            for message in self.queue:
              print "- " + message.id + ' ' + str(message.acks.values()) + ', clock: ' + str(message.clock)

  # create a message and send it to everyone
  def send_message(self):
    self.message_clock += 1

    # create a new message with an unique id
    message = Message(self.id, 'm' + str(self.id) + str(self.message_clock), 'message', clock=self.message_clock)
    print "\nnode" + str(self.id) + " mandando mensagem " + message.id + ' com clock ' + str(message.clock)

    # append the created message to the queue
    self.queue.append(message)

    # Send the message to all destinations
    for destination in self.destinations:
      # try to send the message 3 times
      tries = 0

      while True:
        try:
          print 'Mandando mensagem ' + message.id + ' para ' + str(destination)

          # open the socket
          meu_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
          server_address = ('localhost', 25000 + destination)
          meu_socket.connect(server_address)

          # send the message
          pacote_codificado = pickle.dumps(message)
          meu_socket.send(pacote_codificado)
          meu_socket.close()

          print 'Mensagem enviada\n'
        except Exception as e:
          meu_socket.close()
          tries += 1
          if tries < 3:
            print 'Erro tentando enviar o ack: ', e
            print 'Tentando novamente!'
            continue
          else:
            print 'Erro tentando enviar o ack: ', e
            print 'Nao deu certo...\n'

        break


# =======================================================================

# Defining the 'receiving messages' thread
def thread_recebe():
  # get the server port
  serverPort = int(sys.argv[1])
  # Create the socket
  serverSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
  try:
    # Listen (and keep listening)
    serverSocket.bind(('',serverPort))
    serverSocket.listen(1)
    while True:
      # Accept a connection
      connectionSocket, addr = serverSocket.accept()
      try:
        # Receive and decode data
        data = connectionSocket.recv(1024)
        message = pickle.loads(data)

        # Update the queue based on the received message
        node.update_queue(message)
      except Exception as e:
        print 'Erro ao receber:', e
  except Exception as e:
    print 'Erro ao abrir o socket:', e

# Defining the 'sending messages' thread
def thread_processo():
  while True:
    try:
      # There's a 20% chance the node will send a message
      # (randrange(10) returns a number from 0 to 9)
      time.sleep(4)
      if (randrange(10) < 2):
          node.send_message()
    except Exception as e:
      print 'Erro ao enviar message: ', e

# Defining the 'sending acks' thread
def thread_acks(message, destinations):
  # send the ack message to all the given destinations
  for destination in destinations:
    # try to send the message 3 times
    tries = 0

    while True:
      try:
        # open the socket
        print '\nMandando ack de ' + str(message.id) + ' para ' + str(destination)
        meu_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ('localhost', 25000 + destination)
        meu_socket.connect(server_address)

        # send the ack
        pacote_codificado = pickle.dumps(message)
        meu_socket.send(pacote_codificado)
        meu_socket.close()

        print 'Ack enviado'
      except Exception as e:
        tries += 1
        if tries < 3:
          print 'Erro tentando enviar o ack: ', e
          print 'Tentando novamente!'
          continue
        else:
          print 'Erro tentando enviar o ack: ', e
          print 'Nao deu certo...\n'

      break

# =======================================================================

global node
node = Node(sys.argv[2])

# Main
def main():
  thread.start_new_thread(thread_processo, ())
  thread.start_new_thread(thread_recebe, ())
  signal.pause()

if __name__ == "__main__":
  sys.exit(main())
