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

class ProtoHackers::SpeedDaemon
  @@camera_conns = {} of Socket::IPAddress => IAmCamera
  @@cameras = {} of UInt16 => TCPSocket
  @@dispatchers_conns = {} of Socket::IPAddress => IAmDispatcher
  @@dispatchers = {} of UInt16 => TCPSocket
  @@tickets = {} of String => Array(Ticket)
  @@ticket_times = {} of String => UInt32
  @@plates = {} of String => {IAmCamera, Plate}
  @@wantheartbeats = {} of TCPSocket => WantHeartbeat
  @@heartbeats = {} of TCPSocket => UInt32

  def process_plate(plate, client)
    previous_camera, previous_plate = @@plates[plate.plate]? || {nil, nil}
    if previous_camera && previous_plate
      puts "Has been seen before, checking speed..."
      duration_diff = (plate.timestamp - previous_plate.timestamp).abs / 3600
      current_camera = @@camera_conns[client.remote_address]
      distance_diff = (current_camera.mile - previous_camera.mile).abs
      speed = distance_diff // duration_diff
      if speed > (current_camera.limit + 0.5)
        puts "#{plate.plate} Speeding! #{speed} mph, over limit by #{speed - current_camera.limit} mph"
        ticket = nil
        if plate.timestamp > previous_plate.timestamp
          ticket = Ticket.new()
          ticket.plate = plate.plate
          ticket.road = current_camera.road
          ticket.mile1 = previous_camera.mile
          ticket.timestamp1 = previous_plate.timestamp
          ticket.mile2 = current_camera.mile
          ticket.timestamp2 = plate.timestamp
          ticket.speed = speed * 100
        else
          ticket = Ticket.new()
          ticket.plate = plate.plate
          ticket.road = current_camera.road
          ticket.mile1 = current_camera.mile
          timestamp1 = plate.timestamp
          mile2 = previous_camera.mile
          timestamp2 = previous_plate.timestamp 
          speed = speed * 100
        end

        puts "Ticket created: #{ticket.inspect}"
        
        dispatched = false
        @@dispatchers.each do |socket|
          road, dispatcher_socket = socket
          if road == current_camera.road
            last_ticket_time = @@ticket_times[plate.plate]?
            if last_ticket_time && (Time.utc.to_unix - last_ticket_time) < 86400
              puts "Last ticket was less than 1 day ago, not dispatching"
            else
              puts "Dispatching ticket to #{dispatcher_socket.remote_address}"
              ticket.write(dispatcher_socket)
              @@ticket_times[plate.plate] = Time.utc.to_unix.to_u32
              dispatched = true
            end
            break
          end
        end

        if !dispatched
          puts "No dispatcher for road #{current_camera.road}, saving ticket"
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
    puts "Error processing plate: #{ex}"
  end

  def handle_client(client)
    puts "New connection from #{client.remote_address}"

    while !client.closed? && client.peek != nil && client.peek.size > 0 && client.peek[0] != -1
      puts "Reading type #{client.peek[0].to_u8}"
      case client.peek[0].to_u8
      when 0x20_u8
        if !@@camera_conns[client.remote_address]?
          puts "Client is not a camera"
          error = Error.new()
          error.msg = "Client is not a camera"
          error.write(client)
        else
          plate = client.read_bytes(Plate)
          puts "Plate received #{plate.inspect}"
          process_plate(plate, client)
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
        elsif @@dispatchers_conns[client.remote_address]?
          puts "Already registered as a Dispatcher #{client.remote_address}"
          error = Error.new()
          error.msg = "Already registered as a Dispatcher"
          error.write(client)
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
        elsif @@dispatchers_conns[client.remote_address]?
          puts "Already registered as a Dispatcher #{client.remote_address}"
          error = Error.new()
          error.msg = "Already registered as a Dispatcher"
          error.write(client)
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
                if ticket.road == road
                  last_ticket_time = @@ticket_times[plate]?
                  if last_ticket_time && (Time.utc.to_unix - last_ticket_time) < 86400
                    puts "Last ticket was less than 1 day ago, not dispatching"
                    toRemove << plate
                  else
                    puts "Dispatching ticket for plate #{plate}"
                    ticket.write(client)
                    toRemove << plate
                  end
                end
              end
            end
            toRemove.each do |plate|
              puts "Removing dispatched ticket for plate #{plate}"
              @@tickets.delete(plate)
            end
          end
        end
      else
        puts "Unknown message type #{client.peek[0]}"
        client.close
      end
    end

    if @@camera_conns[client.remote_address]?
      puts "Camera #{client.remote_address} disconnected"
      camera = @@camera_conns.delete(client.remote_address)
      @@cameras.delete(camera.road) if !camera.nil?
    end

    if @@dispatchers_conns[client.remote_address]?
      puts "Dispatcher #{client.remote_address} disconnected"
      dispatcher = @@dispatchers_conns.delete(client.remote_address)
      if !dispatcher.nil?
        dispatcher.roads.each do |road|
          @@dispatchers.delete(road)
        end
      end
    end

    if @@wantheartbeats[client.remote_address]?
      puts "WantHeartbeat for #{client.remote_address} removed"
      wantheartbeat = @@wantheartbeats.delete(client.remote_address)
      @@heartbeats.delete(client) if !wantheartbeat.nil?
    end

    client.close unless client.closed?
    puts "Connection closed for #{client.remote_address}"
  rescue ex
    puts "Error handling client: #{ex}"
    client.close
  end

  def heartbeat()
    puts "Started Heartbeat loop"
    while true
      @@wantheartbeats.each do |socket, wantheartbeat|
        last_heartbeat = @@heartbeats[socket]?
        if last_heartbeat.nil?
          if wantheartbeat.interval/10 > 0
            puts "Initial heartbeat for #{socket.remote_address}"
            heartbeat = Heartbeat.new
            begin
              heartbeat.write(socket)
              @@heartbeats[socket] = Time.utc.to_unix.to_u32
            rescue ex
              puts "Error sending heartbeat: #{ex}"
              socket.close
              @@wantheartbeats.delete(socket)
            end
          end
        elsif wantheartbeat.interval > 0 && Time.utc.to_unix - last_heartbeat > wantheartbeat.interval/10
          puts "Heartbeat for #{socket.remote_address}"
          heartbeat = Heartbeat.new
          begin
            heartbeat.write(socket)
            @@heartbeats[socket] = Time.utc.to_unix.to_u32
          rescue ex
            puts "Error sending heartbeat: #{ex}"
            socket.close
            @@wantheartbeats.delete(socket)
            @@heartbeats.delete(socket)
          end
        end
      end
      sleep 1
    end
  rescue ex
    puts "Error in heartbeat loop: #{ex}"
  end

  def initialize(host, port)
    puts "Starting Speed Daemon server on #{host}:#{port}"
    server = TCPServer.new(host, port, 1000, true, true)
    server.tcp_nodelay = true
    spawn heartbeat()
    while client = server.accept?
      spawn handle_client(client)
    end
  end
end