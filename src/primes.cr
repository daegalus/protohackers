require "big"

module Primes
  module MillerRabin

    # Returns true if +self+ is a prime number, else returns false.
    def primemr?(k = 10)
      primes = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47}
      return primes.includes? self if self <= primes.last
      modp47 = 614_889_782_588_491_410.to_big_i      # => primes.product, largest < 2^64
      return false if modp47.gcd(self.to_big_i) != 1 # eliminates 86.2% of all integers
      # Choose input witness bases: wits = [range, [wit_bases]] or nil
      wits = WITNESS_RANGES.find { |range, wits| range > self }
      witnesses = wits && wits[1] || k.times.map{ 2 + rand(self - 4) }
      miller_rabin_test(witnesses)
    end

    # Returns true if +self+ passes Miller-Rabin Test on witnesses +b+
    private def miller_rabin_test(witnesses) # list of witnesses for testing
      neg_one_mod = n = d = self - 1 # these are even as self is always odd
      d >>= d.trailing_zeros_count   # shift out factors of 2 to make d odd
      witnesses.each do |b|          # do M-R test with each witness base
        next if (b % self) == 0      # **skip base if a multiple of input**
        y = powmod(b, d, self)       # y = (b**d) mod self
        s = d
        until y == 1 || y == neg_one_mod || s == n
          y = (y * y) % self         # y = (y**2) mod self
          s <<= 1
        end
        return false unless y == neg_one_mod || s.odd?
      end
      true
    end

    # Best known deterministic witnesses for given range and set of bases
    # https://miller-rabin.appspot.com/
    # https://en.wikipedia.org/wiki/Miller%E2%80%93Rabin_primality_test
    private WITNESS_RANGES = {
      341_531 => {9345883071009581737},
      1_050_535_501 => {336781006125, 9639812373923155},
      350_269_456_337 => {4230279247111683200, 14694767155120705706, 16641139526367750375},
      55_245_642_489_451 => {2, 141889084524735, 1199124725622454117, 11096072698276303650},
      7_999_252_175_582_851 => {2, 4130806001517, 149795463772692060, 186635894390467037,
                                3967304179347715805},
      585_226_005_592_931_977 => {2, 123635709730000, 9233062284813009, 43835965440333360,
                                  761179012939631437, 1263739024124850375},
      18_446_744_073_709_551_615 => {2, 325, 9375, 28178, 450775, 9780504, 1795265022},
      "318665857834031151167461".to_big_i  => {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37},
      "3317044064679887385961981".to_big_i => {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41}
    }

    # Compute b**e mod m
    private def powmod(b, e, m)
      r, b = 1, b.to_big_i
      while e > 0
        r = (b * r) % m if e.odd?
        b = (b * b) % m
        e >>= 1
      end
      r
    end
  end
end

struct Int; include Primes::MillerRabin end
# struct Int64; include Primes::MillerRabin end
# struct BigInt; include Primes::MillerRabin end