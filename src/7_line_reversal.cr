require "socket"
require "big"
require "bindata"
require "io"

def putfs(session_id, s)
  puts s
  File.open("log/#{session_id}.log", "a") do |file|
    file.print "#{s}\n"
  end
end

abstract class ProtoHackers::LRMessage
  property ip : Socket::IPAddress, command : String, session_id : String, ordinal : UInt32, data : String, resends : Int32
  def initialize(@ip, @command, @session_id, @ordinal : UInt32 = 0, @data = "", @resends = 0)
  end

  abstract def send(server : UDPSocket)
end

class ProtoHackers::ConnectMessage < ProtoHackers::LRMessage
  def initialize(ip, session_id)
    super(ip, "connect", session_id)
  end

  def send(server : UDPSocket, ordinal : UInt32 = 0)
    putfs(@session_id, "--> ip:#{@ip} c:#{@command} s:#{@session_id} r:#{@resends}")
    server.send("/#{@command}/#{@session_id}/", to: @ip)
  end
end

class ProtoHackers::DataMessage < ProtoHackers::LRMessage
  def initialize(ip, session_id, ordinal, data)
    super(ip, "data", session_id, ordinal, data)
  end

  def send(server : UDPSocket)
    putfs(@session_id, "--> ip:#{@ip} c:#{@command} s:#{@session_id} o:#{@ordinal} r:#{@resends}\n#{@data}")
    server.send("/#{@command}/#{@session_id}/#{@ordinal}/#{@data.gsub("/", "\\/").gsub("\\", "\\\\")}/", to: @ip)
  end
end

class ProtoHackers::AckMessage < ProtoHackers::LRMessage
  def initialize(ip, session_id, ordinal : UInt32)
    super(ip, "ack", session_id, ordinal)
  end

  def send(server : UDPSocket)
    putfs(@session_id, "--> ip:#{@ip} c:#{@command} s:#{@session_id} o:#{@ordinal} r:#{@resends}")
    server.send("/#{@command}/#{@session_id}/#{@ordinal}/", to: @ip)
  end
end

class ProtoHackers::CloseMessage < ProtoHackers::LRMessage
  def initialize(ip, session_id)
    super(ip, "close", session_id)
  end

  def send(server : UDPSocket)
    putfs(@session_id, "--> ip:#{@ip} c:#{@command} s:#{@session_id} r:#{@resends}")
    server.send("/#{@command}/#{@session_id}/", to: @ip)
  end
end

class ProtoHackers::LRSession
  property ip : Socket::IPAddress, session_id : String, data : String, bytes_sent : UInt32, last_message_time : Int64, unacked : Array(LRMessage), reversed_data : String, acked : Array(LRMessage)

  def initialize(@ip, @session_id)
    @data = ""
    @reversed_data = ""
    @bytes_sent = 0
    @last_message_time = Time.utc.to_unix + 3
    @unacked = Array(LRMessage).new
    @acked = Array(LRMessage).new
  end

  def getLargestAckedOrdinal() : UInt32
    if @acked.size == 0
      return 0.to_u32
    end
    return @acked.max_by(&.ordinal).ordinal
  end
end

class ProtoHackers::LRCPServer
  @@sessions = {} of Socket::IPAddress => ProtoHackers::LRSession
  @@send_queue = [] of ProtoHackers::LRMessage

  property server : UDPSocket

  def initialize(@server : UDPSocket)
  end

  def outbound()
    while true
      if @@send_queue.size > 0
        @@send_queue.shift.send(@server)
      else
        sleep 0.1
      end
    end
  end

  def decode_message(ip : Socket::IPAddress, data : String) : LRMessage | Nil
    return nil if !data.starts_with?("/") && !data.ends_with?("/")

    if data.size < 3
      return nil
    end

    data = data[1..-2]

    command, session_id, ordinal, body = "", "", "", ""
    msg = data.split("/", 4)

    if msg.size == 2
      command, session_id = msg
      return nil if command != "connect" && command != "close"
    elsif msg.size == 3
      command, session_id, ordinal = msg
      return nil if command != "ack"
    elsif msg.size == 4
      command, session_id, ordinal, body = msg
      return nil if command != "data"

      return nil if body =~ /(?:[^\\])\//
    end

    return nil if session_id.to_u32?.nil?

    case command
    when "connect"
      ProtoHackers::ConnectMessage.new(ip, session_id)
    when "close"
      ProtoHackers::CloseMessage.new(ip, session_id)
    when "ack"
      return nil if ordinal.to_u32?.nil?
      ProtoHackers::AckMessage.new(ip, session_id, ordinal.to_u32)
    when "data"
      return nil if ordinal.to_u32?.nil?
      body = body.gsub("\\/", "/").gsub("\\\\", "\\")
      ProtoHackers::DataMessage.new(ip, session_id, ordinal.to_u32, body)
    else
      putfs(session_id, "[?] #{command}")
      nil
    end
  end

  def handle_request(ip : Socket::IPAddress, data : String)
    message = decode_message(ip, data)
    return if message.nil?
    putfs(message.session_id, "<-- ip:#{ip} c:#{message.command} s:#{message.session_id} o:#{message.ordinal} r:#{message.resends}\n#{message.data}")

    case message
    when ProtoHackers::ConnectMessage
      @@sessions[ip] = ProtoHackers::LRSession.new(ip, message.session_id) if @@sessions[ip]?.nil?
      @@send_queue << ProtoHackers::AckMessage.new(ip, message.session_id, 0)
    when ProtoHackers::CloseMessage
      @@sessions.delete(ip)
      @@send_queue << message
    when ProtoHackers::AckMessage
      @@send_queue << ProtoHackers::CloseMessage.new(ip, message.session_id) if @@sessions[ip]?.nil?

      session = @@sessions[ip]

      if message.ordinal < session.getLargestAckedOrdinal
        putfs(message.session_id, "<!> ordinal #{message.ordinal} < acked #{session.getLargestAckedOrdinal}")
        return
      end

      if message.ordinal < session.bytes_sent
        putfs(message.session_id, "<<< ordinal #{message.ordinal} rsize #{session.reversed_data.size}")
        @@send_queue << ProtoHackers::DataMessage.new(ip, message.session_id, message.ordinal, session.reversed_data[message.ordinal..-1])
        return
      end

      if message.ordinal > session.bytes_sent
        putfs(message.session_id, "<!! invalid")
        @@sessions.delete(ip)
        @@send_queue << ProtoHackers::CloseMessage.new(ip, message.session_id)
        return
      end

      session.last_message_time = Time.utc.to_unix
      to_remove = [] of ProtoHackers::LRMessage
      session.unacked.each do |m|
        to_remove << m if m.ordinal <= message.ordinal
        putfs(session.session_id, "!!> adding to remove #{m}") if session.getLargestAckedOrdinal
      end
      to_remove.each do |m|
        unacked_msg = session.unacked.delete(m)
        session.acked << unacked_msg unless unacked_msg.nil?
      end

    when ProtoHackers::DataMessage
      @@send_queue << ProtoHackers::CloseMessage.new(ip, message.session_id) if @@sessions[ip]?.nil?

      session = @@sessions[ip]

      if message.ordinal < session.bytes_sent
        putfs(message.session_id, "<#> duplicate data")
        return
      end

      if message.ordinal > session.data.size
        putfs(message.session_id, "<?? missing data")
        @@send_queue << ProtoHackers::AckMessage.new(ip, message.session_id, session.data.size.to_u32)
        return
      end

      return if message.data.size + message.ordinal < session.data.size
      start_pos = session.data.size - message.ordinal
      trimmed_data = message.data[start_pos..]
      putfs(message.session_id, "<?> #{message.data.gsub("\n", "\\n")}")
      putfs(message.session_id, "<?> #{session.data.gsub("\n", "\\n")}")
      session.data += trimmed_data
      putfs(message.session_id, "<?> #{trimmed_data.gsub("\n", "\\n")}")
      putfs(message.session_id, "<?> #{session.data.gsub("\n", "\\n")}")
      @@send_queue << ProtoHackers::AckMessage.new(ip, message.session_id, session.data.size.to_u32)

      putfs(message.session_id, "!n> #{session.data[-1] == '\n'}")
      if session.data.size > 0 && session.data[-1] == '\n'
        reversed_data = reverse_message(session.data, session.session_id)
        putfs(message.session_id, "!r> #{reversed_data.gsub("\n", "\\n")}")
        data_message = ProtoHackers::DataMessage.new(ip, message.session_id, session.bytes_sent, reversed_data)
        session.reversed_data = reversed_data
        session.bytes_sent = session.reversed_data.size.to_u32
        session.unacked << data_message
        @@send_queue << data_message
      end

    else
      putfs(message.session_id, "[?] ip:#{ip} c:#{message.command} s:#{message.session_id} o:#{message.ordinal} r:#{message.resends}")
    end
  end

  def reverse_message(message : String, session_id : String)
    putfs(session_id, "%n> #{message.size}")
    lines = message.split("\n")
    reversed_lines = lines.map! { |l| l.reverse }
    joined_lines = reversed_lines.join("\n")
    putfs(session_id, "%n> #{joined_lines.size}")
    return joined_lines
  end

  def unacked_reaper()
    @@sessions.each do |ip, session|
      to_remove = [] of ProtoHackers::LRMessage
      session.unacked.each do |unacked|
        if unacked.ordinal >= session.getLargestAckedOrdinal
          current_time = Time.utc.to_unix
          if unacked.resends > 20
            to_remove << unacked
          elsif current_time - session.last_message_time > 3
            @@send_queue << unacked
            session.last_message_time = current_time
            unacked.resends += 1
          end
        else
          acked = session.unacked.delete(unacked)
          session.acked << acked unless acked.nil?
        end
      end
      to_remove.each do |unacked|
        session.unacked.delete(unacked)
      end
    end
    sleep 3.seconds
  end
end

class ProtoHackers::LineReversal
  def handle_client(lrcp : ProtoHackers::LRCPServer)
    data, ip = lrcp.server.receive(1000)
    lrcp.handle_request(ip, data)
  end

  def initialize(host : String, port : Int32)
    puts "Starting Line Reversal server on #{host}:#{port}"

    Dir.each_child("log") do |file|
      File.delete("log/#{file}")
    end
    server = UDPSocket.new
    server.bind host, port

    spawn do
      loop do
        ProtoHackers::LRCPServer.new(server).unacked_reaper()
      end
    end

    lrcp = ProtoHackers::LRCPServer.new(server)
    spawn lrcp.outbound()

    loop do
      handle_client(lrcp)
    end

  end
end
