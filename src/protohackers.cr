require "./0_smoke_test"
require "./1_primetime"
require "./2_means_to_an_end"
require "./3_budget_chat"
require "./4_unusual_db"
require "./5_mob_in_the_middle"
require "./6_speed_daemon"
require "./7_line_reversal"

module ProtoHackers
  VERSION = "0.1.0"

  def self.run
    spawn ProtoHackers::SmokeTest.new("0.0.0.0", 10001)
    spawn ProtoHackers::PrimeTime.new("0.0.0.0", 10002)
    spawn ProtoHackers::MeansToAnEnd.new("0.0.0.0", 10003)
    spawn ProtoHackers::BudgetChat.new("0.0.0.0", 10004)
    spawn ProtoHackers::UnusualDB.new("0.0.0.0", 10005)
    spawn ProtoHackers::MobInTheMiddle.new("0.0.0.0", 10006)
    spawn ProtoHackers::SpeedDaemon.new("0.0.0.0", 10007)
    spawn ProtoHackers::LineReversal.new("0.0.0.0", 10008)

    sleep
  end
end

ProtoHackers.run
