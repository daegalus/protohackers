require "socket"
require "big"
require "bindata"
require "io"


class ProtoHackers::BudgetChat
  @@users = {} of String => TCPSocket

  def handle_client(client)
    puts "New client connected"
    client.puts "Welcome to BudgetChat! What's your name?"
    name = client.gets
    if name.nil?
      client.puts "You didn't enter a name. Goodbye."
      client.close
      return
    end

    if !validate_username(client, name)
      client.close
      return
    end

    puts "New user: #{name}"
    broadcast "* #{name} has joined the chat", name
    client.puts "* The room contains: #{@@users.keys.join(", ")}"
    @@users[name] = client

    while line = client.gets
      broadcast "[#{name}] #{line}", name
    end

    @@users.delete(name)
    broadcast "* #{name} has left the chat", name
    puts "Client disconnected"

    client.close unless client.closed?
    puts "Connection closed for #{client.remote_address}"
  end

  def validate_username(client : TCPSocket, name : String)
    if @@users.includes? name
      client.puts "Sorry, that name is already taken."
      return false
    end

    if name.empty?
      client.puts "Sorry, you must enter a name."
      return false
    end

    if name.size > 20
      client.puts "Sorry, that name is too long."
      return false
    end

    if name =~ /[^a-zA-Z0-9_\-]/
      client.puts "Sorry, that name contains invalid characters."
      return false
    end

    return true
  end

  def broadcast(message, exclude = nil)
    @@users.each do |user, client|
      if user != exclude
        client.puts message
      end
    end
  end

  def initialize(host, port)
    puts "Starting Budget Chat server on #{host}:#{port}"
    server = TCPServer.new(host, port, 1000, true, true)
    server.tcp_nodelay = true
    while client = server.accept?
      spawn handle_client(client)
    end
  end
end
