require "socket"

class ProtoHackers::VoraciousCodeStorage
  def handle_client(client : TCPSocket, storage : ProtoHackers::Storage)
    client_id = "#{client.remote_address.address}:#{client.remote_address.port}"
    client.puts("READY")
    while line = client.gets
      request = line.split(" ")
      next if request.empty?

      command = request[0].downcase

      if command == "get"
        if request.size < 2
          ProtoHackers::Log.error &.emit("GET too small", {:client_id => client_id, :request => request})
          client.puts "ERR usage: GET file [revision]"
          client.puts "READY"
          next
        end

        path = request[1]
        revision = request[2]?
        if !storage.valid_file_path?(path)
          ProtoHackers::Log.error &.emit("GET invalid filename", {:client_id => client_id, :request => request})
          client.puts "ERR illegal file name"
          next
        end

        file = storage[path]?

        if file
          rev = file.get_revision?(revision) if revision
          rev = file.get_revision? unless revision

          if rev
            client.puts "OK #{rev.size}"
            client.puts rev
          else
            ProtoHackers::Log.error &.emit("GET no such revision", {:client_id => client_id, :file => path, :revision => revision})
            client.puts "ERR no such revision"
            client.puts "READY"
            next
          end
        else
          ProtoHackers::Log.error &.emit("GET no such file", {:client_id => client_id, :file => path})
          client.puts "ERR no such file"
        end
      elsif command == "put"
        if request.size < 2
          ProtoHackers::Log.error &.emit("PUT too short", {:client_id => client_id, :request => request})
          client.puts "ERR usage: PUT file"
          client.puts "READY"
          next
        end

        if request.size > 3
          ProtoHackers::Log.error &.emit("PUT too long", {:client_id => client_id, :request => request})
          client.puts "ERR usage: PUT file length newline data"
          client.puts "READY"
          next
        end

        path = request[1]
        size = request[2]
        size = 0 if size.to_i?.nil?
        if !storage.valid_file_path?(path)
          ProtoHackers::Log.error &.emit("PUT illegal file name", {:client_id => client_id, :request => request})
          client.puts "ERR illegal file name"
          next
        end

        file = storage[path]?

        begin
          data = client.read_string(size.to_i)
        rescue ex
          ProtoHackers::Log.error &.emit("PUT data read failed", {:client_id => client_id, :file => path, :error => ex.to_s})
          #client.close unless client.closed?
          next
        end

        if file
          #ProtoHackers::Log.info &.emit("PUT request", {:client_id => client_id, :file => path})
          file.add_revision(data)
          client.puts "OK #{file.latest_revision}"
        else
          success = storage[path] = data
          client.puts "OK #{storage[path]?.not_nil!.latest_revision}" if success
          client.puts "ERR non-text content" unless success
        end
      elsif command == "list"
        if request.size < 2
          ProtoHackers::Log.error &.emit("LIST too short", {:client_id => client_id, :request => request})
          client.puts "ERR usage: LIST dir"
          client.puts "READY"
          next
        end

        prefix = request[1]
        if !storage.valid_dir?(prefix)
          ProtoHackers::Log.error &.emit("LIST invalid dir", {:client_id => client_id, :request => request})
          client.puts "ERR illegal dir name"
          next
        end

        dir_list = storage.list(prefix)
        client.puts "OK #{dir_list.size}"
        dir_list.sort_by!(&.name).each do |item|
          client.puts "#{item}"
        end
      elsif command == "help"
        client.puts "OK usage: HELP|GET|PUT|LIST"
      else
        ProtoHackers::Log.error &.emit("Unknown command", {:client_id => client_id, :command => command})
        client.puts "ERR illegal method: #{command}"
      end

      client.puts "READY"
    end
  rescue ex
    client_id = "#{client.remote_address.address}:#{client.remote_address.port}"
    ProtoHackers::Log.error &.emit("Error handling client", {:error => ex.to_s, :client_id => client_id})
  ensure
    client.close
  end

  def initialize(host, port)
    ProtoHackers::Log.info &.emit("Starting Voracious Code Storage server", {:host => host, :port => port})
    server = TCPServer.new(host, port, 1000)
    server.tcp_nodelay = true

    storage = ProtoHackers::Storage.new
    while client = server.accept?
      spawn handle_client(client, storage)
    end

    ProtoHackers::Log.info { "Voracious Code Storage server shutting down..." }
  end
end

class ProtoHackers::Storage
  def initialize
    @root = ProtoHackers::Folder.new("/")
  end

  def list(prefix) : Array(ProtoHackers::File | ProtoHackers::Folder)
    # List files for current folder based on prefix from root
    if valid_dir?(prefix)
      if prefix == "/"
        intersection_of_names = @root.files.map(&.name) & @root.folders.map(&.name)
        dir_list = [] of ProtoHackers::File | ProtoHackers::Folder
        dir_list += @root.files
        dir_list += @root.folders.reject { |f| intersection_of_names.includes?(f.name) }
        return dir_list
      end

      prefix = prefix[0..-2] if prefix.ends_with?("/")
      prefix = prefix[1..-1] if prefix.starts_with?("/")

      path_parts = prefix.split("/")

      folder = @root
      path_parts.each do |part|
        folder = folder.get_folder?(part)
        return [] of ProtoHackers::File | ProtoHackers::Folder unless folder
      end

      intersection_of_names = folder.files.map(&.name) & folder.folders.map(&.name)
      dir_list = [] of ProtoHackers::File | ProtoHackers::Folder
      dir_list += folder.files
      dir_list += folder.folders.reject { |f| intersection_of_names.includes?(f.name) }

      return dir_list
    end
    [] of ProtoHackers::File | ProtoHackers::Folder
  end

  def []?(path : String) : ProtoHackers::File | Nil
    # Get file by full path from root traversing folders
    if valid_file_path?(path)
      path = path[1..-1]

      path_parts = path.split("/")
      file_name = path_parts.pop

      folder = @root
      path_parts.each do |part|
        folder = folder.get_folder?(part)
        return nil unless folder
      end

      return folder.get_file?(file_name)
    end

    nil
  end

  def []=(path : String, data : String) : Bool
    # Put file by full path from root traversing folders
    if valid_file_path?(path)
      if data.chars.any? { |c| c.ord < 32 && ![9, 10, 11, 13].includes?(c.ord) || c.ord > 127 }
        return false
      end
      path = path[1..-1]

      path_parts = path.split("/")
      file_name = path_parts.pop

      folder = @root
      path_parts.each do |part|
        subfolder = folder.get_folder?(part)
        unless subfolder
          subfolder = ProtoHackers::Folder.new(part)
          folder.add_folder(subfolder)
        end
        folder = subfolder
      end

      file = folder.get_file?(file_name)
      unless file
        file = ProtoHackers::File.new(file_name, data)
        folder.add_file(file)
      else
        file.add_revision(data)
      end
    end
    return true
  end

  def valid_file_path?(path : String)
    return false if path == "/"

    char_valid = /^[a-zA-Z0-9_\-\.\/]+$/.match(path)
    return false unless char_valid

    empty_check = path
    empty_check = empty_check[1..-1] if empty_check.starts_with?("/")

    return empty_check.split("/").none?(&.empty?) &&  path.starts_with?("/") && !path.ends_with?("/")
  end

  def valid_dir?(path : String)
    return true if path == "/"

    char_valid = /^[a-zA-Z0-9_\-\.\/]+$/.match(path)
    return false unless char_valid

    empty_check = path
    empty_check = empty_check[0..-2] if empty_check.ends_with?("/")
    empty_check = empty_check[1..-1] if empty_check.starts_with?("/")

    return empty_check.split("/").none?(&.empty?) && path.starts_with?("/")
  end
end



class ProtoHackers::Folder
  property name : String, files : Array(ProtoHackers::File), folders : Array(ProtoHackers::Folder)

  def initialize(@name)
    @files = Array(ProtoHackers::File).new
    @folders = Array(ProtoHackers::Folder).new
  end

  def add_file(file : ProtoHackers::File)
    @files << file
  end

  def add_folder(folder : ProtoHackers::Folder)
    @folders << folder
  end

  def get_file?(name : String) : ProtoHackers::File | Nil
    @files.find { |file| file.name == name }
  end

  def get_folder?(name : String) : ProtoHackers::Folder | Nil
    @folders.find { |folder| folder.name == name }
  end

  def list : Array(ProtoHackers::File | ProtoHackers::Folder)
    @files + @folders
  end

  def to_s(io)
    io << "#{@name}/ DIR"
  end
end

class ProtoHackers::File
  property name : String, latest_revision : String, revisions : Hash(String, String), revision_counter : Int32 = 1

  def initialize(@name : String, data : String)
    @latest_revision = "r#{@revision_counter}"
    @revisions = Hash(String, String).new
    @revisions[@latest_revision] = data
    @revision_counter += 1
  end

  def add_revision(data : String)
    if @revisions[@latest_revision] == data
      return
    end
    @latest_revision = "r#{@revision_counter}"
    @revisions[@latest_revision] = data
    @revision_counter += 1
  end

  def get_revision?(revision_id : String = "latest") : String | Nil
    return @revisions[@latest_revision] if revision_id == "latest"
    @revisions[revision_id]?
  end

  def delete_latest_revision
    @revisions.delete(@latest_revision)
    @latest_revision = @revisions.keys.sort!.last
  end

  def to_s(io)
    io << "#{@name} #{@latest_revision}"
  end
end
