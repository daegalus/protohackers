require "socket"

class ProtoHackers::JobCentre
  def handle_client(client : TCPSocket, job_manager : ProtoHackers::JobCentre::Manager)
    #ProtoHackers::Log.info { "New client connected" }
    client_id = "#{client.remote_address.address}:#{client.remote_address.port}"
    while line = client.gets
      request = nil

      begin
        request = Request.from_json(line)
      rescue ex
        client.puts Response.invalid_request
        break
      end

      if !request.is_valid?
        client.puts Response.invalid_request
        next
      end

      case request.request
      when "put"
        unless request.queue.nil? || request.pri.nil? || request.job.nil?
          id = job_manager.add_job(request.queue.not_nil!, request.pri.not_nil!, request.job.not_nil!)
          client.puts Response.get_response(id.to_s, request.job.not_nil!, request.pri.not_nil!, request.queue.not_nil!)
        end
      when "get"
        loop do
          break if request.queues.nil?
          job = job_manager.get_job?(client_id, request.queues.not_nil!)
          if job.nil? && request.wait
            sleep 0.1.seconds
            break if client.closed?
            next
          end

          if job.nil?
            client.puts Response.no_job
            break
          end

          client.puts Response.get_response(job.id.to_s, job.payload, job.priority, job.queue)
          break
        end
      when "delete"
        if !request.id.nil? && job_manager.delete_job(request.id.not_nil!)
          client.puts Response.ok
        else
          client.puts Response.invalid_request
        end
      when "abort"
        if !request.id.nil? && job_manager.abort_job(client_id, request.id.not_nil!)
          client.puts Response.ok
        else
          client.puts Response.invalid_request
        end
      end
    end

    job_manager.on_disconnect(client_id)
    #ProtoHackers::Log.info { "Client disconnected" }
  end

  def initialize(host, port)
    ProtoHackers::Log.info &.emit("Starting Job Centre server", {:host => host, :port => port})
    server = TCPServer.new(host, port, 1000)
    server.tcp_nodelay = true

    job_manager = ProtoHackers::JobCentre::Manager.new
    while client = server.accept?
      spawn handle_client(client, job_manager)
    end

    ProtoHackers::Log.info { "Job center server shutting down..." }
  end
end

class ProtoHackers::JobCentre::Request
  include JSON::Serializable

  property request : String

  property queue : String?
  property pri : Int32?
  property job : Hash(String, String)?

  property queues : Array(String)?
  property wait : Bool?

  property id : UInt64?

  def initialize(@request, @queue, @pri, @job, @queues, @wait, @id)
  end

  def is_valid?
    case @request
    when "put"
      !@queue.nil? && !@pri.nil?
    when "get"
      !@queues.nil?
    when "delete"
      !@id.nil?
    when "abort"
      !@id.nil?
    else
      false
    end
  end
end

class ProtoHackers::JobCentre::Response
  def self.invalid_request
    %({"status": "error", "error": "invalid request"})
  end

  def self.no_job
    %({"status": "no-job"})
  end

  def self.ok
    %({"status": "ok"})
  end

  def self.get_response(id : String, job : Hash(String, String), priority : Int32, queue : String)
    %({"status": "ok", "id": "#{id}", "job": #{job.to_json}, "pri": "#{priority}", "queue": "#{queue}"})
  end
end

class ProtoHackers::JobCentre::Job
  property id : UInt64, queue : String, priority : Int32, payload : Hash(String, String)

  def initialize(@id, @queue, @priority, @payload)
  end
end

class ProtoHackers::JobCentre::Manager
  property queues : Hash(String, Array(Job)) = Hash(String, Array(Job)).new
  property in_progress : Hash(String, Array(Job)) = Hash(String, Array(Job)).new
  property id : UInt64 = 0

  def add_job(queue : String, priority : Int32, payload : Hash(String, String)) : UInt64
    id = @id += 1

    job = Job.new(id, queue, priority, payload)
    queues[job.queue] ||= Array(Job).new
    queues[job.queue] << job

    queues[job.queue].sort! { |a, b| b.priority <=> a.priority }

    id
  end

  def get_job?(worker_id : String, queues : Array(String)) : Job | Nil
    best_queue = ""
    best_priority = -1

    queues.each do |queue|
      if @queues.has_key?(queue) && !@queues[queue].empty? && @queues[queue].first.priority > best_priority
        best_queue = queue
        best_priority = @queues[queue].first.priority
      end
    end

    return nil if best_priority < 0

    job = @queues[best_queue].shift
    in_progress[worker_id] ||= Array(Job).new
    in_progress[worker_id] << job
    job
  end

  def delete_job(job_id : UInt64) : Bool
    @queues.each do |queue, jobs|
      jobs.each do |job|
        if job.id == job_id
          @queues[queue].delete(job)
          @queues[queue].sort! { |a, b| b.priority <=> a.priority }
          return true
        end
      end
    end

    in_progress.each do |worker_id, jobs|
      jobs.each do |job|
        if job.id == job_id
          in_progress[worker_id].delete(job)
          return true
        end
      end
    end

    false
  end

  def abort_job(worker_id : String, job_id : UInt64) : Bool
    in_progress[worker_id].each do |job|
      if job.id == job_id
        in_progress[worker_id].delete(job)
        queues[job.queue] ||= Array(Job).new
        queues[job.queue] << job
        queues[job.queue].sort! { |a, b| b.priority <=> a.priority }
        return true
      end
    end if in_progress.has_key?(worker_id)

    false
  end

  def on_disconnect(worker_id : String)
    in_progress[worker_id].each do |job|
      queues[job.queue] ||= Array(Job).new
      queues[job.queue] << job
      queues[job.queue].sort! { |a, b| b.priority <=> a.priority }
    end if in_progress.has_key?(worker_id)

    in_progress.delete(worker_id)
  end
end
