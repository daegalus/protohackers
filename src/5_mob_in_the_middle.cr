require "socket"
require "big"
require "bindata"
require "io"
require "regex"


class ProtoHackers::MobInTheMiddle
  def handle_client(client, chat)
    puts "New client connected"
    while true
      if chat.closed?
        puts "Proxy client disconnected"
        client.close unless client.closed?
        break
      end
      if client.closed?
        puts "Client disconnected"
        chat.close unless chat.closed?
        break
      end
      while line = client.gets
        message = rewrite_boguscoin_address(line)
        chat.puts message
        puts "Old line: #{line}\nNew line: #{message}"
      end
    end
    puts "Client Connection closed for #{client.remote_address}"
  end

  def rewrite_boguscoin_address(message)
    target_address = "7YWHMfk9JZe0LM0g1ZauHuiSxhI"
    regex = /([7][a-zA-Z0-9]{25,35})/

    if message =~ regex
      message = message.gsub(regex, target_address)
    end

    return message
  end

  def handle_chat(chat, client)
    puts "New proxy client connected"
    while true
      if chat.closed?
        puts "Proxy client disconnected"
        client.close unless client.closed?
        break
      end
      if client.closed?
        puts "Client disconnected"
        chat.close unless chat.closed?
        break
      end
      while line = chat.gets
        message = rewrite_boguscoin_address(line)
        client.puts message
        puts "Old line: #{line}\nNew line: #{message}"
      end
    end

    puts "Proxy connection closed for #{client.remote_address}"
  end

  def initialize(host, port)
    puts "Starting Budget Chat Proxy server on #{host}:#{port}"
    server = TCPServer.new(host, port, 1000, true, true)
    server.tcp_nodelay = true

    while true
      while client = server.accept?
        puts "Creating chat proxy for #{client.remote_address}..."
        chat = TCPSocket.new("chat.protohackers.com", "16963")
        chat.tcp_nodelay = true
        spawn handle_client(client, chat)
        spawn handle_client(chat, client)
      end
    end
  end
end
