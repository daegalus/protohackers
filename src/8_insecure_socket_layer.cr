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

record ProtoHackers::ISLSession, client : TCPSocket, remote_address : String, ciphers : Array(Cipher), accumulator = [] of UInt8, message = [] of UInt8, processing_message = "", processed_pos = 0 do
  def message=(value)
    @message = value
  end

  def accumulator=(value)
    @accumulator = value
  end

  def process_message
    log = ProtoHackers::Log.for("isl")
    #log.info &.emit("###", {:message => @processing_message})
    return if @processing_message.empty? #|| decoded_message.index("\n").nil?

    items = @processing_message.split(",").reject(&.empty?).map{|s| {count: s[/\d+/].to_i, original_string: s}}
    response = items.max_by{|m| m[:count]}
    return if response.nil?
    response = response[:original_string]
    response += "\n" unless response.ends_with?("\n")

    log.info &.emit("<r>", {:address => @client.remote_address.inspect, :ciphers => ciphers.map(&.to_s).join(", "), :response => response})

    encoded_response = ProtoHackers::InsecureSocketLayer.apply_ciphers(@ciphers, response.bytes, @processed_pos.to_u32)
    encoded_slice = Slice(UInt8).new(encoded_response.size)
    encoded_response.each_with_index { |b, i| encoded_slice[i] = b }

    @client.write encoded_slice unless @client.closed?
    @processed_pos += @processing_message.size
  end

  def handle
    log = ProtoHackers::Log.for("isl")
    while !@client.closed?
      buf = Bytes.new(5000)
      len = @client.read(buf)
      msg = buf[0, len]

      sleep 0.1 if msg.empty?
      next if msg.empty?

      #puts msg.hexdump
      decoded_message = ProtoHackers::InsecureSocketLayer.apply_ciphers(@ciphers.reverse, msg.to_a, @accumulator.size.to_u32, is_decode: true)
      #log.info &.emit("<<<", {:message => decoded_message.map(&.unsafe_chr).join})
      @accumulator += msg.to_a
      @message += decoded_message
      newline_index = @message[@processed_pos...].map(&.unsafe_chr).join.index("\n")
      next if newline_index.nil?

      newline_index = @processed_pos+newline_index+1
      @processing_message = @message[@processed_pos...newline_index].map(&.unsafe_chr).join
      process_message
    end
  end
end

class ProtoHackers::InsecureSocketLayer
  @sessions = {} of String => ProtoHackers::ISLSession

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
          elsif cipher_message.size > 2 && cipher_message[-2] != CipherType::XOR.to_u8 && cipher_message[-2] != CipherType::ADD.to_u8
            log.info &.emit("<//")
            break
          end
        else
          log.info &.emit("<//")
          break
        end
      end

      cipher_message << message_byte
    end
    log.info &.emit("<##", {:message => cipher_message.inspect})

    ciphers = parse_ciphers(cipher_message)

    client.close if ciphers.empty? || ciphers.first.cipher_type == CipherType::END
    return if client.closed?

    client.close if ProtoHackers::InsecureSocketLayer.apply_ciphers(ciphers.reverse, CIPHER_TEST.bytes, 0) == CIPHER_TEST.bytes
    return if client.closed?
    log.info &.emit("~~~", {:ciphers => ciphers.map(&.to_s).join(", ")})

    session = @sessions[client.remote_address.to_s] ||= ProtoHackers::ISLSession.new(client, client.remote_address.to_s, ciphers, [] of UInt8, [] of UInt8)
    @sessions[client.remote_address.to_s] = session if @sessions[client.remote_address.to_s].nil?

    log.info &.emit("&&&", {:session => session.remote_address.inspect, :ciphers => session.ciphers.map(&.to_s).join(", ")})
    session.handle
    log.info &.emit("///", {:session => session.remote_address.inspect, :ciphers => session.ciphers.map(&.to_s).join(", ")})
    session.client.close unless session.client.closed?
    @sessions.delete(session.remote_address)
  rescue ex : Exception
    log = ProtoHackers::Log.for("isl")
    log.error &.emit("!!!", {:exception => ex.inspect, :backtrace => ex.backtrace.join("\n")})
    @sessions.delete(session.remote_address) unless session.nil?
    client.close unless client.closed?
  end

  def self.reverse_bits(byte : UInt8) : UInt8
    table = [
      0x00, 0x80, 0x40, 0xc0, 0x20, 0xa0, 0x60, 0xe0,
      0x10, 0x90, 0x50, 0xd0, 0x30, 0xb0, 0x70, 0xf0,
      0x08, 0x88, 0x48, 0xc8, 0x28, 0xa8, 0x68, 0xe8,
      0x18, 0x98, 0x58, 0xd8, 0x38, 0xb8, 0x78, 0xf8,
      0x04, 0x84, 0x44, 0xc4, 0x24, 0xa4, 0x64, 0xe4,
      0x14, 0x94, 0x54, 0xd4, 0x34, 0xb4, 0x74, 0xf4,
      0x0c, 0x8c, 0x4c, 0xcc, 0x2c, 0xac, 0x6c, 0xec,
      0x1c, 0x9c, 0x5c, 0xdc, 0x3c, 0xbc, 0x7c, 0xfc,
      0x02, 0x82, 0x42, 0xc2, 0x22, 0xa2, 0x62, 0xe2,
      0x12, 0x92, 0x52, 0xd2, 0x32, 0xb2, 0x72, 0xf2,
      0x0a, 0x8a, 0x4a, 0xca, 0x2a, 0xaa, 0x6a, 0xea,
      0x1a, 0x9a, 0x5a, 0xda, 0x3a, 0xba, 0x7a, 0xfa,
      0x06, 0x86, 0x46, 0xc6, 0x26, 0xa6, 0x66, 0xe6,
      0x16, 0x96, 0x56, 0xd6, 0x36, 0xb6, 0x76, 0xf6,
      0x0e, 0x8e, 0x4e, 0xce, 0x2e, 0xae, 0x6e, 0xee,
      0x1e, 0x9e, 0x5e, 0xde, 0x3e, 0xbe, 0x7e, 0xfe,
      0x01, 0x81, 0x41, 0xc1, 0x21, 0xa1, 0x61, 0xe1,
      0x11, 0x91, 0x51, 0xd1, 0x31, 0xb1, 0x71, 0xf1,
      0x09, 0x89, 0x49, 0xc9, 0x29, 0xa9, 0x69, 0xe9,
      0x19, 0x99, 0x59, 0xd9, 0x39, 0xb9, 0x79, 0xf9,
      0x05, 0x85, 0x45, 0xc5, 0x25, 0xa5, 0x65, 0xe5,
      0x15, 0x95, 0x55, 0xd5, 0x35, 0xb5, 0x75, 0xf5,
      0x0d, 0x8d, 0x4d, 0xcd, 0x2d, 0xad, 0x6d, 0xed,
      0x1d, 0x9d, 0x5d, 0xdd, 0x3d, 0xbd, 0x7d, 0xfd,
      0x03, 0x83, 0x43, 0xc3, 0x23, 0xa3, 0x63, 0xe3,
      0x13, 0x93, 0x53, 0xd3, 0x33, 0xb3, 0x73, 0xf3,
      0x0b, 0x8b, 0x4b, 0xcb, 0x2b, 0xab, 0x6b, 0xeb,
      0x1b, 0x9b, 0x5b, 0xdb, 0x3b, 0xbb, 0x7b, 0xfb,
      0x07, 0x87, 0x47, 0xc7, 0x27, 0xa7, 0x67, 0xe7,
      0x17, 0x97, 0x57, 0xd7, 0x37, 0xb7, 0x77, 0xf7,
      0x0f, 0x8f, 0x4f, 0xcf, 0x2f, 0xaf, 0x6f, 0xef,
      0x1f, 0x9f, 0x5f, 0xdf, 0x3f, 0xbf, 0x7f, 0xff,
    ].map(&.to_u8)
    table[byte]
    # byte = (byte & 0xF0) >> 4 | (byte & 0x0F) << 4
    # byte = (byte & 0xCC) >> 2 | (byte & 0x33) << 2
    # byte = (byte & 0xAA) >> 1 | (byte & 0x55) << 1
    # byte
  end

  def self.apply_ciphers(ciphers : Array(Cipher), bytes : Array(UInt8), pos : UInt32, is_decode : Bool = false) : Array(UInt8)
    log = ProtoHackers::Log.for("isl")
    byte_copy = bytes.clone
    ciphers.each do |cipher|
      case cipher.cipher_type
      when CipherType::REVERSE
        byte_copy = byte_copy.map { |c| reverse_bits(c) }
      when CipherType::XOR
        byte_copy = byte_copy.map { |c| (c ^ cipher.extra_info) }
      when CipherType::XORPOS
        byte_copy = byte_copy.map_with_index { |c, i| (c ^ ((pos.to_u8! &+ i.to_u8!)).to_u8!) }
      when CipherType::ADD
        byte_copy = byte_copy.map(&.&+ cipher.extra_info) if !is_decode
        byte_copy = byte_copy.map(&.&- cipher.extra_info) if is_decode
      when CipherType::APOS
        byte_copy = byte_copy.map_with_index { |c, i| c &+ (pos.to_u8! &+ i.to_u8!) } if !is_decode
        byte_copy = byte_copy.map_with_index { |c, i| c &- (pos.to_u8! &+ i.to_u8!) } if is_decode
      else
        log.warn { "Unxpected cipher type: #{cipher.cipher_type}" }
      end
    end

    byte_copy
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

  def initialize(num : UInt8)
  end
end
