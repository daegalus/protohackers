require "log"

require "./0_smoke_test"
require "./1_primetime"
require "./2_means_to_an_end"
require "./3_budget_chat"
require "./4_unusual_db"
require "./5_mob_in_the_middle"
require "./6_speed_daemon"
require "./7_line_reversal"
require "./8_insecure_socket_layer"

module ProtoHackers
  VERSION = "0.1.0"

  SFBackend = SessionFileBackend.new("", "log/ph.log", "a")
  MemBackend = ::Log::MemoryBackend.new

  ::Log.setup do |c|
    c.bind "ph", :debug, ::Log::IOBackend.new
    c.bind "ph.user.*", :debug, MemBackend
    c.bind "ph.isl", :debug, ::Log::IOBackend.new
  end

  Log = ::Log.for("ph")

  spawn do
    Signal::INT.trap do
      Log.info { "Exiting..." }
      session_ids = MemBackend.entries.map { |e| e.data[:session_id] }.uniq

      session_ids.each do |session_id|
        SFBackend.session_id = session_id.to_s
        session_entries = MemBackend.entries.select { |e| e.data[:session_id] == session_id }
        session_entries.each {|se| SFBackend.write(se)}
      end

      SFBackend.close
      exit
    end
  end

  def self.run
    Log.info &.emit("Starting ProtoHackers", version: VERSION)
    # spawn ProtoHackers::SmokeTest.new("0.0.0.0", 10001)
    # spawn ProtoHackers::PrimeTime.new("0.0.0.0", 10002)
    # spawn ProtoHackers::MeansToAnEnd.new("0.0.0.0", 10003)
    # spawn ProtoHackers::BudgetChat.new("0.0.0.0", 10004)
    # spawn ProtoHackers::UnusualDB.new("0.0.0.0", 10005)
    # spawn ProtoHackers::MobInTheMiddle.new("0.0.0.0", 10006)
    # spawn ProtoHackers::SpeedDaemon.new("0.0.0.0", 10007)
    # spawn ProtoHackers::LineReversal.new("0.0.0.0", 10008)
    spawn ProtoHackers::InsecureSocketLayer.new("0.0.0.0", 10009)
    sleep
  end
end

ProtoHackers.run



class SessionFileBackend < ::Log::IOBackend
  def session_id=(@session_id : String)
    @io = File.new("log/user.#{@session_id}.log", "a")
  end

  def initialize(@session_id : String, file_name : String, mode : String = "a", *, formatter : Log::Formatter = Log::ShortFormat)
    super File.new(file_name, mode), formatter: formatter, dispatcher: :sync
  end

  def initialize(@session_id : String, file : File, *, formatter : Log::Formatter = Log::ShortFormat)
    super file, formatter: formatter, dispatcher: :sync
  end

  def close
    @io.close
    super
  end
end
