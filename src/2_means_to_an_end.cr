require "socket"
require "big"
require "bindata"
require "io"

class Data < BinData
  endian big

  uint8 :char, default: 0
  int32 :num1
  int32 :num2
end

class Response < BinData
  endian big

  int32 :mean, default: 0
end

class ProtoHackers::MeansToAnEnd
  def handle_client(client)
    store = {} of Int32 => Int32
    puts "New connection from #{client.remote_address}"

    while !client.closed? && client.peek != nil && client.peek.size > 0 && client.peek[0] != -1
      data = client.read_bytes(Data)
      
      #puts "Received from #{client.remote_address}: #{data.inspect}"
      if data.char == 'I'.ord
        store[data.num1] = data.num2
      elsif data.char == 'Q'.ord
        ranged = store.select { |k, v| k >= data.num1 && k <= data.num2 }
        sum = ranged.reduce(BigInt.new(0)) { |sum, (k, v)| sum += v }
        mean = 0.0
        mean = sum / ranged.size unless ranged.size == 0


        response = Response.new

        begin
          response.mean = mean.to_i32
        rescue
        end
        response.write(client)
      else
        puts "Unknown command: #{data.char.chr}"
      end
    end

    client.close unless client.closed?
    puts "Connection closed for #{client.remote_address}"
  end

  def initialize(host, port)
    puts "Starting Means server"
    server = TCPServer.new(host, port, 1000, true, true)
    server.tcp_nodelay = true
    while client = server.accept?
      spawn handle_client(client)
    end
  end
end
