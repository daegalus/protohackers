require "socket"
require "big"
require "bindata"
require "io"

class Error < BinData
  endian big

  uint8 :type, default: 0x10
  uint8 :msg_len, value: ->{ msg.bytesize }
  string :msg, length: ->{msg_len}
end

class Plate < BinData
  endian big

  uint8 :type, default: 0x20
  uint8 :plate_len, value: ->{ plate.bytesize }
  string :plate, length: ->{plate_len}
  uint32 :timestamp
end

class Ticket < BinData
  endian big

  uint8 :type, default: 0x21
  uint8 :plate_len, value: ->{ plate.bytesize }
  string :plate, length: ->{plate_len}
  uint16 :road
  uint16 :mile1
  uint32 :timestamp1
  uint16 :mile2
  uint32 :timestamp2
  uint16 :speed
end

class WantHeartbeat < BinData
  endian big

  uint8 :type, default: 0x40
  uint32 :interval
end

class Heartbeat < BinData
  endian big

  uint8 :type, default: 0x41
end

class IAmCamera < BinData
  endian big

  uint8 :type, default: 0x80
  uint16 :road
  uint16 :mile
  uint16 :limit
end

class IAmDispatcher < BinData
  endian big

  uint8 :type, default: 0x81
  uint8 :numroads, default: 0
  array roads : UInt16, length: ->{numroads}
end

class PlateSighting < BinData
  endian big

  uint8 :plate_len, value: ->{ plate.bytesize }
  string :plate, length: ->{plate_len}
  uint16 :road
  uint16 :mile
  uint32 :timestamp
end

class ProtoHackers::SpeedDaemon
  @@camera_conns = {} of Socket::IPAddress => IAmCamera
  @@cameras = {} of UInt16 => TCPSocket
  @@dispatchers_conns = {} of Socket::IPAddress => IAmDispatcher
  @@dispatchers = {} of UInt16 => TCPSocket
  @@tickets = {} of String => Array(Ticket)
  @@ticket_times = {} of String => Array(UInt32)
  @@plates = {} of String => {IAmCamera, Plate}
  @@plate_sightings = {} of String => Array(PlateSighting)
  @@wantheartbeats = {} of TCPSocket => WantHeartbeat
  @@heartbeats = {} of Socket::IPAddress => UInt32

  def process_plate(plate, client, current_camera)
    #previous_camera, previous_plate = @@plates[plate.plate]? || {nil, nil}
    prev_plate = @@plate_sightings[plate.plate]?
    prev_plate = prev_plate.last? if prev_plate
    if prev_plate
      #puts "Has been seen before, checking speed..."
      begin
        duration_diff_seconds = plate.timestamp.to_i64! - prev_plate.timestamp.to_i64!
        duration_diff = duration_diff_seconds.abs.to_u32! / 3600
        distance_diff = (current_camera.mile.to_i32! - prev_plate.mile.to_i32!).abs.to_u16!
        speed = distance_diff / duration_diff
      rescue ex : OverflowError
        puts "OverflowError: #{ex.message}"
        puts "Plate: #{plate.plate}"
        puts "Previous plate: #{prev_plate.plate}"
        puts "Current camera: #{current_camera.inspect}"
        puts "Plate timestamp: #{plate.timestamp}"
        puts "Previous plate timestamp: #{prev_plate.timestamp}"
        puts "Duration diff seconds: #{duration_diff_seconds}"
        puts "Duration diff: #{duration_diff}"
        puts "Distance diff: #{distance_diff}"
        puts "Speed: #{speed}"
        return
      end
      if speed > (current_camera.limit + 0.5)
        puts "#{plate.plate} Speeding! #{speed.round(2)} mph, over limit by #{speed.round(2) - current_camera.limit} mph"
        ticket = nil
        speed = (speed.round(2) * 100).to_u16
        if plate.timestamp > prev_plate.timestamp
          ticket = Ticket.new()
          ticket.plate = plate.plate
          ticket.road = current_camera.road
          ticket.mile1 = prev_plate.mile
          ticket.timestamp1 = prev_plate.timestamp
          ticket.mile2 = current_camera.mile
          ticket.timestamp2 = plate.timestamp
          ticket.speed = speed
        else
          ticket = Ticket.new()
          ticket.plate = plate.plate
          ticket.road = current_camera.road
          ticket.mile1 = current_camera.mile
          ticket.timestamp1 = plate.timestamp
          ticket.mile2 = prev_plate.mile
          ticket.timestamp2 = prev_plate.timestamp
          ticket.speed = speed
        end

        puts "Ticket created: #{ticket.inspect}"

        dispatched = false
        @@dispatchers.each do |socket|
          road, dispatcher_socket = socket
          dispatched = dispatch_ticket(plate.plate, ticket, dispatcher_socket) if road == current_camera.road
        end

        if !dispatched
          #puts "No dispatcher for road #{current_camera.road}, saving ticket"
          if @@tickets[plate.plate]?
            @@tickets[plate.plate] << ticket
          else
            @@tickets[plate.plate] = [ticket]
          end
        end
      end
    else
        camera = @@camera_conns[client.remote_address]?
        @@plates[plate.plate] = {camera, plate} if camera
    end
  rescue ex
    puts "Error processing plate: #{ex.inspect}"
  end

  def dispatch_ticket(plate, ticket, dispatcher)
    day1 = ticket.timestamp1 // 86400
    #puts "Day1: #{day1}"
    day2 = ticket.timestamp2 // 86400
    #puts "Day2: #{day2}"
    skip = false
    (day1..day2).each do |day|
      #puts "Checking if dispatchable for day #{day}"
      if !@@ticket_times[plate]?
        @@ticket_times[plate] = [] of UInt32
      elsif @@ticket_times[plate].includes?(day)
        #puts "Already dispatched a ticket for this plate on this day"
        skip = true
      end
    end
    return false if skip
    (day1..day2).each do |day|
      #puts "Adding if dispatchable for day #{day}"
      if !@@ticket_times[plate]?
        @@ticket_times[plate] = [day]
      else
        @@ticket_times[plate] << day
      end
    end
    #puts "Dispatching ticket to #{dispatcher.remote_address}"
    ticket.write(dispatcher)
    return true
  end

  def handle_client(client)
    #puts "New connection from #{client.remote_address}"

    while !client.closed? && client.peek != nil && client.peek.size > 0 && client.peek[0] != -1
      #puts "Reading type #{client.peek[0].to_u8}"
      case client.peek[0].to_u8
      when 0x20_u8
        if !@@camera_conns[client.remote_address]?
          puts "Client is not a camera"
          error = Error.new()
          error.msg = "Client is not a camera"
          error.write(client)
          client.close unless client.closed?
        else
          plate = client.read_bytes(Plate)
          puts "Plate received #{plate.inspect}"
          camera = @@camera_conns[client.remote_address]
          plate_sighting = PlateSighting.new()
          plate_sighting.plate = plate.plate
          plate_sighting.road = camera.road
          plate_sighting.mile = camera.mile
          plate_sighting.timestamp = plate.timestamp
          process_plate(plate, client, camera)
          @@plate_sightings[plate.plate] = [plate_sighting] if !@@plate_sightings[plate.plate]?
          @@plate_sightings[plate.plate] << plate_sighting if @@plate_sightings[plate.plate]?
          @@plate_sightings[plate.plate] = @@plate_sightings[plate.plate].sort_by!(&.timestamp)
        end
      when 0x40_u8
        if @@wantheartbeats[client]?
          puts "Already have a WantHeartbeat for #{client.remote_address}"
          error = Error.new()
          error.msg = "Already registered as a Dispatcher"
          error.write(client)
        else
          wantHeartbeat = client.read_bytes(WantHeartbeat)
          @@wantheartbeats[client] = wantHeartbeat
          puts "WantHeartBeat received #{wantHeartbeat.inspect}"
        end
      when 0x80_u8
        if @@camera_conns[client.remote_address]?
          puts "Already registered as a Camera #{client.remote_address}"
          error = Error.new()
          error.msg = "Already registered as a Camera"
          error.write(client)
          client.close unless client.closed?
        elsif @@dispatchers_conns[client.remote_address]?
          puts "Already registered as a Dispatcher #{client.remote_address}"
          error = Error.new()
          error.msg = "Already registered as a Dispatcher"
          error.write(client)
          client.close unless client.closed?
        else
          camera = client.read_bytes(IAmCamera)
          puts "Camera received #{camera.inspect}"
          @@camera_conns[client.remote_address] = camera
          @@cameras[camera.road] = client
        end
      when 0x81_u8
        if @@camera_conns[client.remote_address]?
          puts "Already registered as a Camera #{client.remote_address}"
          error = Error.new()
          error.msg = "Already registered as a Camera"
          error.write(client)
          client.close unless client.closed?
        elsif @@dispatchers_conns[client.remote_address]?
          puts "Already registered as a Dispatcher #{client.remote_address}"
          error = Error.new()
          error.msg = "Already registered as a Dispatcher"
          error.write(client)
          client.close unless client.closed?
        else
          dispatcher = client.read_bytes(IAmDispatcher)
          puts "Dispatcher received #{dispatcher.inspect}"
          @@dispatchers_conns[client.remote_address] = dispatcher
          dispatcher.roads.each do |road|
            @@dispatchers[road] = client
          end

          dispatcher.roads.each do |road|
            puts "Dispatcher #{client.remote_address} checking for tickets for road #{road}"
            toRemove = [] of String
            @@tickets.each do |plate, tickets|
              tickets.each do |ticket|
                dispatched = false
                dispatched = dispatch_ticket(plate, ticket, client) if ticket.road == road
                toRemove << plate if dispatched
              end
            end
            toRemove.each do |plate|
              puts "Removing dispatched ticket for plate #{plate}"
              @@tickets.delete(plate)
            end
          end
        end
      else
        #puts "Unknown message type #{client.peek[0]}"
        error = Error.new()
        error.msg = "Unknown message type"
        error.write(client)
        client.close unless client.closed?
      end
    end

    if @@camera_conns[client.remote_address]?
      #puts "Camera #{client.remote_address} disconnected"
      camera = @@camera_conns.delete(client.remote_address)
      @@cameras.delete(camera.road) if !camera.nil?
    end

    if @@dispatchers_conns[client.remote_address]?
      #puts "Dispatcher #{client.remote_address} disconnected"
      dispatcher = @@dispatchers_conns.delete(client.remote_address)
      if !dispatcher.nil?
        dispatcher.roads.each do |road|
          @@dispatchers.delete(road)
        end
      end
    end

    if @@wantheartbeats[client.remote_address]?
      #puts "WantHeartbeat for #{client.remote_address} removed"
      wantheartbeat = @@wantheartbeats.delete(client.remote_address)
      @@heartbeats.delete(client) if !wantheartbeat.nil?
    end

    client.close unless client.closed?
    #puts "Connection closed for #{client.remote_address}"
  rescue ex
    puts "Error handling client: #{ex.inspect}"
    client.close
  end

  def heartbeat()
    #puts "Started Heartbeat loop"
    socket_outer = nil
    loop do
      @@wantheartbeats.each do |socket, wantheartbeat|
        socket_outer = socket
        last_heartbeat = @@heartbeats[socket.remote_address]?
        if last_heartbeat.nil?
          if wantheartbeat.interval/10 > 0
            heartbeat = Heartbeat.new
            begin
              heartbeat.write(socket)
              @@heartbeats[socket.remote_address] = Time.utc.to_unix.to_u32
            rescue ex
              #puts "Error sending heartbeat: #{ex.inspect}"
              socket.close unless socket.closed?
              @@wantheartbeats.delete(socket)
            end
          end
        elsif wantheartbeat.interval > 0 && Time.utc.to_unix - last_heartbeat > wantheartbeat.interval/10
          heartbeat = Heartbeat.new
          begin
            heartbeat.write(socket)
            @@heartbeats[socket.remote_address] = Time.utc.to_unix.to_u32
          rescue ex
            #puts "Error sending heartbeat: #{ex.inspect}"
            socket.close unless socket.closed?
            @@wantheartbeats.delete(socket)
            @@heartbeats.delete(socket.remote_address)
          end
        end
      end
      sleep Time::Span.new(nanoseconds: 500_000) # 100ms
    end
  rescue ex
    #puts "Error in heartbeat loops: #{ex.inspect}"
    @@wantheartbeats.delete(socket_outer) unless socket_outer.nil?
  end

  def initialize(host, port)
    puts "Starting Speed Daemon server on #{host}:#{port}"
    server = TCPServer.new(host, port, 1000, true, true)
    server.tcp_nodelay = true
    spawn do
      loop do
        heartbeat()
      end
    end
    while client = server.accept?
      spawn handle_client(client)
    end
  end
end
