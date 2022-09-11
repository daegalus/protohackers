require "socket"

class ProtoHackers::SmokeTest
  def handle_client(client)
    begin
      puts "New connection from #{client.remote_address}"
      message = client.getb_to_end
      puts "[#{client.remote_address}] Message: #{message}"
      client.write(message)
      client.close
      puts "Connection closed."
    rescue e
      puts "Client disconnected: #{client.remote_address}"
    end
  end

  def initialize(host, port)
    puts "Starting SmokeTest server"
    server = TCPServer.new(host, port, 1000, true, true)
    server.tcp_nodelay = true
    while client = server.accept?
      spawn handle_client(client)
    end
  end
end
