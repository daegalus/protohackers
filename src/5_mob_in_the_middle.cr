require "socket"
require "big"
require "bindata"
require "io"
require "regex"


class ProtoHackers::MobInTheMiddle
  def handle_proxy(src, dest, is_server)
    while line = src.gets(chomp: false)
      if line.ends_with?("\n")
        message = rewrite_boguscoin_address(line)
        dest.puts message
      end
    end

    src.close unless src.closed?
    dest.close unless dest.closed?

    puts "Connection closed for #{src.remote_address}"
  end

  def rewrite_boguscoin_address(message)
    target_address = "7YWHMfk9JZe0LM0g1ZauHuiSxhI"
    regex = /(^|(?<= ))7[a-zA-Z0-9]{25,34}($|(?= ))/

    if message =~ regex
      puts "Old Line: #{message}"
      message = message.gsub(regex, target_address)
      puts "New Line: #{message}"
    end

    return message
  end

  def initialize(host, port)
    puts "Starting Budget Chat Proxy server on #{host}:#{port}"
    server = TCPServer.new(host, port, 1000, true, true)
    server.tcp_nodelay = true

    while client = server.accept?
      puts "Creating chat connection for #{client.remote_address}..."
      chat = TCPSocket.new("chat.protohackers.com", "16963")
      chat.tcp_nodelay = true
      spawn handle_proxy(client, chat, false)
      spawn handle_proxy(chat, client, true)
    end

    puts "Proxy server shutting down..."
  end
end
