require "socket"
require "json"
require "big"
require "./primes"

struct Message
  include JSON::Serializable

  module NumConverter
    def self.from_json(pull : JSON::PullParser) : Int64 | BigInt
      if !pull.kind.int? && !pull.kind.float?
        raise JSON::ParseException.new("Expected int but got #{pull.kind}", *pull.location)
      end

      begin
        if pull.kind.int?
          value = pull.int_value
          pull.read_next
          return value
        elsif pull.kind.float?
          value = pull.int_value
          pull.read_next
          return value
        end
      rescue
        # OK - fallback to bigfloat
      end

      value = pull.read_raw
      if value.includes?('.')
        return BigFloat.new(value).to_big_i
      else
        return BigInt.new(value)
      end
    end
  end

  getter method : String
  @[JSON::Field(converter: Message::NumConverter)]
  getter number : Int64 | BigInt
end

class ProtoHackers::PrimeTime
  def handle_client(client)
    begin
      puts "New connection from #{client.remote_address}"
      while message = client.gets
        puts "Received: #{message}"
        msg = Message.from_json(message)
        if msg.method == "isPrime"
          resp = {
            "method" => "isPrime",
            "prime"  => msg.number.primemr?,
          }
          client.puts resp.to_json
        else
          resp = {"error" => "Invalid method"}
          client.puts resp.to_json
          puts "Method Error, Sent: #{resp.to_json}"
          break
        end
      end
      client.close
      puts "Connection closed."
    rescue e
      resp = {"error" => "Invalid message"}
      client.puts resp.to_json
      puts "Error: #{e.message}"
      puts "Client disconnected: #{client.remote_address}"
    end
  end

  def is_prime?(num : Number | BigFloat | BigInt)
    puts "Getting Prime"
    return false if num <= 1
    Math.sqrt(num).to_i.downto(2) { |i| return false if num % i == 0 }
    true
  end

  def initialize(host, port)
    puts "Starting PrimeTime server"
    server = TCPServer.new(host, port, 1000, true, true)
    server.tcp_nodelay = true
    while client = server.accept?
      spawn handle_client(client)
    end
  end
end
