require "./0_smoke_test"
require "./1_primetime"
require "./2_means_to_an_end"

module ProtoHackers
  VERSION = "0.1.0"

  def self.run
    spawn ProtoHackers::SmokeTest.new("0.0.0.0", 10001)
    spawn ProtoHackers::PrimeTime.new("0.0.0.0", 10002)
    spawn ProtoHackers::MeansToAnEnd.new("0.0.0.0", 10003)
    
    sleep
  end
end

ProtoHackers.run