require "socket"
require "big"
require "bindata"
require "io"
require "regex"
require "bit_array"
require "log"

enum CipherType : UInt8
  END     = 0x00
  REVERSE = 0x01
  XOR     = 0x02
  XORPOS  = 0x03
  ADD     = 0x04
  APOS    = 0x05
end

CIPHER_TEST = "127x turnips\n"

record Cipher, cipher_type : CipherType, extra_info : UInt8 = 0 do
  def to_s
    if extra_info == 0
      "#{cipher_type.to_s}"
    else
      "#{cipher_type.to_s} #{extra_info}"
    end
  end
end

class ProtoHackers::InsecureSocketLayer
  def handle_client(client : TCPSocket)
    log = ProtoHackers::Log.for("isl")
    log.info &.emit("---", {:address => client.remote_address.inspect})

    cipher_message = [] of UInt8
    client.each_byte do |message_byte|
      if message_byte == CipherType::END.to_u8
        if cipher_message.size > 0
          if cipher_message[-1] != CipherType::XOR.to_u8 && cipher_message[-1] != CipherType::ADD.to_u8
            log.info &.emit("<//")
            break
          elsif cipher_message[-2] != CipherType::XOR.to_u8 && cipher_message[-2] != CipherType::ADD.to_u8
            log.info &.emit("<//")
            break
          end
        end
      end

      cipher_message << message_byte
    end
    log.info &.emit("<##", {:message => cipher_message.inspect})

    ciphers = parse_ciphers(cipher_message)

    client.close if apply_ciphers(ciphers.reverse, CIPHER_TEST.bytes, 0) == CIPHER_TEST
    log.info &.emit("~~~", {:ciphers => ciphers.map(&.to_s).join(", ")})

    accumulator = [] of UInt8
    message = [] of UInt8
    resp_pos = 0
    client.each_byte do |message_byte|
        #log.info &.emit("...", {:message_byte => message_byte.to_s})
        accumulator << message_byte
        #log.info &.emit("...", {:accumulator => accumulator.join})

        decoded_message = apply_ciphers(ciphers.reverse, [message_byte], accumulator.size)
        log.info &.emit("...", {:decoded_message => decoded_message.map(&.unsafe_chr).join})
        message += decoded_message

        if decoded_message.map(&.unsafe_chr).join == "\n"
          log.info &.emit("-->", {:message => message.map(&.unsafe_chr).join})
          respo_pos = process_message(client, ciphers, message, resp_pos)
          message = [] of UInt8
        end
    end

    client.close unless client.closed?
  end

  def process_message(client : TCPSocket, ciphers : Array(Cipher), decoded_message_bytes : Array(UInt8), pos : Int32)
    log = ProtoHackers::Log.for("isl")

    decoded_message = decoded_message_bytes.map(&.unsafe_chr).join
    log.info &.emit("###", {:message => decoded_message})
    max_match = decoded_message.split(",").map { |s| /(?<count>\d+)x (?<toy>[\w\s\-]+)/.match(s) }.max_by do |m|
      count = m["count"].to_i if m
      count || 0
    end

    log.info &.emit("<m>", {:match => max_match.inspect})
    response = "#{max_match["count"] if max_match}x #{max_match["toy"] if max_match}#{"\n" if max_match && max_match["toy"][-1] != '\n'}"

    log.info &.emit("<r>", {:response => response})

    encoded_response = apply_ciphers(ciphers, response.bytes, pos)
    encoded_slice = Slice(UInt8).new(encoded_response.size)
    encoded_response.each_with_index { |b, i| encoded_slice[i] = b }

    client.write encoded_slice
    return pos + decoded_message.size
  end

  def reverse_bits(byte : UInt8) : UInt8
    byte = (byte & 0xF0) >> 4 | (byte & 0x0F) << 4
    byte = (byte & 0xCC) >> 2 | (byte & 0x33) << 2
    byte = (byte & 0xAA) >> 1 | (byte & 0x55) << 1
    byte
  end

  def apply_ciphers(ciphers : Array(Cipher), bytes : Array(UInt8), pos : Int32) : Array(UInt8)
    log = ProtoHackers::Log.for("isl")
    ciphers.each do |cipher|
      case cipher.cipher_type
      when CipherType::REVERSE

        bytes = bytes.map { |c|
          log.info &.emit("rc.", {:c => c.to_s})
          reverse_bits(c)
        }
        break
      when CipherType::XOR
        bytes = bytes.map { |c| (c ^ (cipher.extra_info % 256)) }
      when CipherType::XORPOS
        bytes = bytes.map_with_index { |c, i| c ^ ((pos + i) % 256) }
      when CipherType::ADD
        bytes = bytes.map(&.&+ cipher.extra_info)
      when CipherType::APOS
        bytes = bytes.map_with_index { |c, i| c &+ ((pos + i) % 256) }
      end
    end

    bytes
  end

  def parse_ciphers(message : Array(UInt8)) : Array(Cipher)
    ciphers = [] of Cipher
    isXOR = false
    isAdd = false
    message.each_with_index do |byte, index|
      if isXOR
        ciphers << Cipher.new(CipherType::XOR, byte)
        isXOR = false
        next
      elsif isAdd
        ciphers << Cipher.new(CipherType::ADD, byte)
        isAdd = false
        next
      end

      if byte == 0
        cipher_end = index
        break
      end

      ciphers << Cipher.new(CipherType::REVERSE, 0) if byte == 1
      isXOR = true if byte == 2
      ciphers << Cipher.new(CipherType::XORPOS, 0) if byte == 3
      isAdd = true if byte == 4
      ciphers << Cipher.new(CipherType::APOS, 0) if byte == 5
    end

    ciphers
  end

  def initialize(host, port)
    ProtoHackers::Log.info &.emit("Starting Insecure Socket Layer server", {:host => host, :port => port})
    server = TCPServer.new(host, port, 1000)
    server.tcp_nodelay = true

    while client = server.accept?
      spawn handle_client(client)
    end

    ProtoHackers::Log.info { "Proxy server shutting down..." }
  end
end
