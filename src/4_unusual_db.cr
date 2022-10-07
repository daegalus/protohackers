require "socket"
require "big"
require "bindata"
require "io"

class ProtoHackers::UnusualDB
  @@db = {} of String => String

  def handle_client(server)
    data, ip = server.receive(1000)

    puts "Received #{data} from #{ip}"

    has_value = !data.index('=').nil?

    if !has_value
      puts "Get command for key #{data}"
      value = @@db[data]?
      if data == "version"
        begin
          server.send("version=yulian's db", to: ip)
        rescue ex : Socket::ConnectError
          puts ex.inspect
        end
      elsif value
        begin
          server.send("#{data}=#{value}", to: ip)
          puts "Sending #{data}=#{value} to #{ip}"
        rescue ex : Socket::ConnectError
          puts ex.inspect
        end
      else
        begin
          server.send("#{data}=", to: ip)
          puts "Sending #{data}= to #{ip}"
        rescue ex : Socket::ConnectError
          puts ex.inspect
        end
      end
    elsif has_value
      idx = data.index('=')
      if !idx.nil? && data[0..idx-1] != "version"
        key, value = data.split('=', limit: 2, remove_empty: false)
        puts "Insert command for key #{key}"
        @@db[key] = value
      end
    end
  end

  def initialize(host, port)
    puts "Starting Unusual DB server on #{host}:#{port}"
    server = UDPSocket.new
    server.bind host, port

    while true
      handle_client(server)
    end
  end
end
