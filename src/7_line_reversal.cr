require "socket"
require "big"
require "bindata"
require "io"
require "log"

CHUNK_SIZE = 500

def setup_session_log(session_id : String)
  ProtoHackers::Log.for("user.#{session_id}")
end

class ProtoHackers::LRState
  property sessions, send_channel

  def initialize
    @sessions = {} of String => ProtoHackers::LRSession
    @send_channel = Channel(ProtoHackers::LRMessage).new
  end

  def +(session : ProtoHackers::LRSession)
    @sessions[session.session_id] = session
  end

  def -(session : ProtoHackers::LRSession)
    @sessions.delete(session.session_id)
  end

  def -(session_id : String)
    @sessions.delete(session_id)
  end

  def <<(message : ProtoHackers::LRMessage)
    @send_channel.send(message)
  end

  def >> : ProtoHackers::LRMessage
    @send_channel.receive
  end

  def [](session_id : String) : ProtoHackers::LRSession
    @sessions[session_id]
  end

  def exists?(session_id : String) : Bool
    @sessions.has_key?(session_id)
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
    log = setup_session_log(@session_id)
    log.info &.emit("-->", {:ip => @ip.to_s, :command => @command, :session_id => @session_id, :resends => @resends.to_s})
    server.send("/#{@command}/#{@session_id}/", to: @ip)
  end
end

class ProtoHackers::DataMessage < ProtoHackers::LRMessage
  def initialize(ip, session_id, ordinal, data)
    super(ip, "data", session_id, ordinal, data)
  end

  def send(server : UDPSocket)
    log = setup_session_log(@session_id)
    log.info &.emit("-->", {:ip => @ip.to_s, :command => @command, :session_id => @session_id, :ordinal => @ordinal.to_s, :resends => @resends.to_s, :data => @data})
    server.send("/#{@command}/#{@session_id}/#{@ordinal}/#{@data.gsub("/", "\\/").gsub("\\", "\\\\")}/", to: @ip)
  end
end

class ProtoHackers::AckMessage < ProtoHackers::LRMessage
  def initialize(ip, session_id, ordinal : UInt32)
    super(ip, "ack", session_id, ordinal)
  end

  def send(server : UDPSocket)
    log = setup_session_log(@session_id)
    log.info &.emit("-->", {:ip => @ip.to_s, :command => @command, :session_id => @session_id, :ordinal => @ordinal.to_s, :resends => @resends.to_s})
    server.send("/#{@command}/#{@session_id}/#{@ordinal}/", to: @ip)
  end
end

class ProtoHackers::CloseMessage < ProtoHackers::LRMessage
  def initialize(ip, session_id)
    super(ip, "close", session_id)
  end

  def send(server : UDPSocket)
    log = setup_session_log(@session_id)
    log.info &.emit("-->", {:ip => @ip.to_s, :command => @command, :session_id => @session_id, :resends => @resends.to_s})
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

  def getLargestAckedOrdinal : UInt32
    return 0.to_u32 if @acked.size == 0
    return @acked.max_by(&.ordinal).ordinal
  end

  def reverse_message
    log = setup_session_log(@session_id)
    log.info &.emit "%n> #{@data.size}", {:session_id => @session_id}
    @reversed_data = @data.split("\n").map { |line| line.reverse }.join("\n")
    log.info &.emit "%n> #{@reversed_data.size}", {:session_id => @session_id}
  end
end

class ProtoHackers::LRCPServer
  property server : UDPSocket, state : ProtoHackers::LRState

  def initialize(@server : UDPSocket, @state : ProtoHackers::LRState)
  end

  def outbound
    loop do
      msg = @state.>>
      begin
        msg.send(@server)
      rescue ex
        log = setup_session_log(msg.session_id)
        log.error &.emit("Error sending message", {:session_id => msg.session_id, :error => ex.message})
      end
    end
  end

  def decode_message(ip : Socket::IPAddress, data : String) : LRMessage | Nil
    data = data.strip("\x00")

    return nil if data.size < 3
    return nil if !data.starts_with?("/") || !data.ends_with?("/")

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
      return nil if body.empty?
    else
      return nil
    end

    return nil if session_id.to_u32?.nil?

    log = setup_session_log(session_id)
    log.info &.emit("<d-", {:ip => ip.to_s, :command => command, :session_id => session_id, :ordinal => ordinal, :data => body})

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
      return nil if body.empty?
      body = body.gsub("\\\\", "\\").gsub("\\/", "/")
      return nil if body.empty?
      ProtoHackers::DataMessage.new(ip, session_id, ordinal.to_u32, body)
    else
      log.info &.emit("[?] Unknown command", {:command => command, :session_id => session_id})
      nil
    end
  end

  def chunks(data : String, chunk_size : Int32 = CHUNK_SIZE) : Array(String)
    return [] of String if data.size == 0
    return [data] if chunk_size >= data.size

    data.each_char.in_groups_of(chunk_size).map(&.join).to_a
  end

  def handle_connect(ip : Socket::IPAddress, message : ProtoHackers::ConnectMessage)
    @state + ProtoHackers::LRSession.new(ip, message.session_id) unless @state.exists?(message.session_id)
    @state << ProtoHackers::AckMessage.new(ip, message.session_id, 0)
  end

  def handle_close(ip : Socket::IPAddress, message : ProtoHackers::CloseMessage)
    @state - message.session_id
    @state << message
  end

  def handle_ack(ip : Socket::IPAddress, message : ProtoHackers::AckMessage)
    @state << ProtoHackers::CloseMessage.new(ip, message.session_id) unless @state.exists?(message.session_id)
    return unless @state.exists?(message.session_id)

    session = @state[message.session_id]
    log = setup_session_log(session.session_id)

    if message.ordinal < session.getLargestAckedOrdinal
      log.warn &.emit "<!> ordinal #{message.ordinal} < acked #{session.getLargestAckedOrdinal}", {:session_id => message.session_id}
      return
    end

    if message.ordinal < session.bytes_sent
      log.info &.emit "<<< ordinal #{message.ordinal} rsize #{session.reversed_data.size}", {:session_id => message.session_id}
      if session.reversed_data[message.ordinal..].size <= CHUNK_SIZE
        data_message = ProtoHackers::DataMessage.new(ip, message.session_id, message.ordinal, session.reversed_data[message.ordinal..])
        session.unacked << data_message
        @state << data_message
      else
        ordinal = message.ordinal
        chunked_data = chunks(session.reversed_data[message.ordinal..])
        chunked_data.each do |chunk|
          data_message = ProtoHackers::DataMessage.new(ip, message.session_id, ordinal, chunk)
          session.unacked << data_message
          @state << data_message
          ordinal += chunk.size
        end
      end
      return
    end

    if message.ordinal > session.bytes_sent
      log.warn &.emit "<!! invalid", {:session_id => message.session_id}
      @state - message.session_id
      @state << ProtoHackers::CloseMessage.new(ip, message.session_id)
      return
    end

    session.last_message_time = Time.utc.to_unix
    to_remove = [] of ProtoHackers::LRMessage
    session.unacked.each do |m|
      to_remove << m if m.ordinal <= message.ordinal
      log.info &.emit "!!> adding to remove #{m}", {:session_id => message.session_id} if session.getLargestAckedOrdinal
    end
    to_remove.each do |m|
      unacked_msg = session.unacked.delete(m)
      session.acked << unacked_msg unless unacked_msg.nil?
    end
  end

  def handle_data(ip : Socket::IPAddress, message : ProtoHackers::DataMessage)
    @state << ProtoHackers::CloseMessage.new(ip, message.session_id) unless @state.exists?(message.session_id)
    return unless @state.exists?(message.session_id)

    session = @state[message.session_id]
    log = setup_session_log(message.session_id)

    if message.ordinal < session.bytes_sent
      log.warn &.emit "<#> duplicate data", {:session_id => message.session_id}
      return
    end

    if message.ordinal > session.data.size
      log.warn &.emit "<?? missing data", {:session_id => message.session_id}
      @state << ProtoHackers::AckMessage.new(ip, message.session_id, session.data.size.to_u32)
      return
    end

    return if message.data.size + message.ordinal < session.data.size
    start_pos = session.data.size - message.ordinal
    trimmed_data = message.data[start_pos..]
    session.data += trimmed_data
    @state << ProtoHackers::AckMessage.new(ip, message.session_id, session.data.size.to_u32)

    log.info &.emit "!n>", {:is_new_line => session.data.chars.last == '\n', :session_id => message.session_id} if !session.data.empty?
    if !session.data.empty? && session.data.chars.last == '\n'
      session.reverse_message
      log.info &.emit "!r>", {:reveresed_data => session.reversed_data.gsub("\n", "\\n"), :session_id => message.session_id}

      if session.reversed_data[session.bytes_sent..].size <= CHUNK_SIZE
        data_message = ProtoHackers::DataMessage.new(ip, message.session_id, session.bytes_sent, session.reversed_data[session.bytes_sent..])
        session.bytes_sent = session.reversed_data.size.to_u32
        session.unacked << data_message
        @state << data_message
      else
        chunked_data = chunks(session.reversed_data[session.bytes_sent..])
        chunked_data.each do |chunk|
          data_message = ProtoHackers::DataMessage.new(ip, message.session_id, session.bytes_sent, chunk)
          session.bytes_sent += chunk.size.to_u32
          session.unacked << data_message
          @state << data_message
        end
      end
    end
  end

  def handle_request(ip : Socket::IPAddress, data : String)
    message = decode_message(ip, data)
    return if message.nil?

    log = setup_session_log(message.session_id)

    log.info &.emit "<--", {:ip => ip.to_s, :command => message.command, :session_id => message.session_id, :ordinal => message.ordinal.to_s, :resends => message.resends.to_s, :data => message.data}

    case message
    when ProtoHackers::ConnectMessage
      handle_connect(ip, message)
    when ProtoHackers::CloseMessage
      handle_close(ip, message)
    when ProtoHackers::AckMessage
      handle_ack(ip, message)
    when ProtoHackers::DataMessage
      handle_data(ip, message)
    else
      log.warn &.emit "[?]", {:ip => ip.to_s, :command => message.command, :session_id => message.session_id, :ordinal => message.ordinal.to_s, :resends => message.resends.to_s}
    end
  end

  def unacked_reaper
    loop do
      @state.sessions.each do |session_id, session|
        to_remove = [] of ProtoHackers::LRMessage
        session.unacked.each do |unacked|
          if unacked.ordinal >= session.getLargestAckedOrdinal
            current_time = Time.utc.to_unix
            if unacked.resends > 20
              to_remove << unacked
            elsif current_time - session.last_message_time > 3
              @state << unacked
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
end

class ProtoHackers::LineReversal
  def handle_client(lrcp : ProtoHackers::LRCPServer)
    data, ip = lrcp.server.receive(1000)
    lrcp.handle_request(ip, data)
  end

  def initialize(host : String, port : Int32)
    ProtoHackers::Log.info &.emit "Starting Line Reversal server", {:host => host, :port => port}
    state = ProtoHackers::LRState.new
    Dir.each_child("log") do |file|
      File.delete("log/#{file}")
    end
    server = UDPSocket.new
    server.bind host, port

    lrcp = ProtoHackers::LRCPServer.new(server, state)

    spawn lrcp.unacked_reaper
    spawn lrcp.outbound

    loop do
      handle_client(lrcp)
    end
  end
end
