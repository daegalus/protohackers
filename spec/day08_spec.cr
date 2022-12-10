require "spec"
require "../src/protohackers"
require "../src/8_insecure_socket_layer"

def create_obj
  ProtoHackers::InsecureSocketLayer.new(0)
end

def create_cipher_xor(num : UInt8)
  Cipher.new(CipherType::XOR, num)
end

def create_cipher_xorpos
  Cipher.new(CipherType::XORPOS, 0)
end

def create_cipher_add(num : UInt8)
  Cipher.new(CipherType::ADD, num)
end

def create_cipher_addpos
  Cipher.new(CipherType::APOS, 0)
end

def create_cipher_reverse_bits
  Cipher.new(CipherType::REVERSE, 0)
end

HELLO_WORLD = [0x68,0x65,0x6c,0x6c,0x6f].map(&.to_u8)
EXPECTED_REVERSE_BITS = [0x16,0xa6,0x36,0x36,0xf6].map(&.to_u8)
EXPECTED_XOR_1 = [0x69,0x64,0x6d,0x6d,0x6e].map(&.to_u8)
EXPECTED_XORPOS = [0x68,0x64,0x6e,0x6f,0x6b].map(&.to_u8)
EXPECTED_XOR_1_REVERSE_BITS = [0x96,0x26,0xb6,0xb6,0x76].map(&.to_u8)
EXPECTED_ADD_1 = [0x69,0x66,0x6d,0x6d,0x70].map(&.to_u8)
EXPECTED_ADDPOS = [0x68,0x66,0x6e,0x6f,0x73].map(&.to_u8)

describe ProtoHackers::InsecureSocketLayer do
  describe "Reverse Bits" do
    it "should reverse bits correctly" do
      ph = create_obj
      ph.reverse_bits(HELLO_WORLD[0]).should eq EXPECTED_REVERSE_BITS[0]
      ph.reverse_bits(HELLO_WORLD[1]).should eq EXPECTED_REVERSE_BITS[1]
      ph.reverse_bits(HELLO_WORLD[2]).should eq EXPECTED_REVERSE_BITS[2]
      ph.reverse_bits(HELLO_WORLD[3]).should eq EXPECTED_REVERSE_BITS[3]
      ph.reverse_bits(HELLO_WORLD[4]).should eq EXPECTED_REVERSE_BITS[4]

      ph.reverse_bits(EXPECTED_REVERSE_BITS[0]).should eq HELLO_WORLD[0]
      ph.reverse_bits(EXPECTED_REVERSE_BITS[1]).should eq HELLO_WORLD[1]
      ph.reverse_bits(EXPECTED_REVERSE_BITS[2]).should eq HELLO_WORLD[2]
      ph.reverse_bits(EXPECTED_REVERSE_BITS[3]).should eq HELLO_WORLD[3]
      ph.reverse_bits(EXPECTED_REVERSE_BITS[4]).should eq HELLO_WORLD[4]
    end
  end

  describe "Apply Ciphers" do
    it "should apply XOR 1, REVERSE_BITS correctly" do
      ph = create_obj
      ph.apply_ciphers([create_cipher_xor(1), create_cipher_reverse_bits], HELLO_WORLD, 0).should eq EXPECTED_XOR_1_REVERSE_BITS
    end

    it "should apply XOR correctly" do
      ph = create_obj
      ph.apply_ciphers([create_cipher_xor(1.to_u8)], HELLO_WORLD, 0).should eq EXPECTED_XOR_1
    end

    it "should apply XOR correctly wrap" do
      ph = create_obj
      ph.apply_ciphers([create_cipher_xor(257.to_u8!)], HELLO_WORLD, 0).should eq EXPECTED_XOR_1
    end

    it "should apply XORPOS correctly" do
      ph = create_obj
      ph.apply_ciphers([create_cipher_xorpos], HELLO_WORLD, 0).should eq EXPECTED_XORPOS
    end

    it "should apply XORPOS correctly wrap" do
      ph = create_obj
      ph.apply_ciphers([create_cipher_xorpos], HELLO_WORLD, 256.to_u8!).should eq EXPECTED_XORPOS
    end

    it "should apply ADD correctly" do
      ph = create_obj
      ph.apply_ciphers([create_cipher_add(1.to_u8)], HELLO_WORLD, 0).should eq EXPECTED_ADD_1
    end

    it "should apply ADD correctly wrap" do
      ph = create_obj
      ph.apply_ciphers([create_cipher_add(257.to_u8!)], HELLO_WORLD, 0).should eq EXPECTED_ADD_1
    end

    it "should apply ADDPOS correctly" do
      ph = create_obj
      ph.apply_ciphers([create_cipher_addpos], HELLO_WORLD, 0).should eq EXPECTED_ADDPOS
    end

    it "should apply ADDPOS correctly wrap" do
      ph = create_obj
      ph.apply_ciphers([create_cipher_addpos], HELLO_WORLD, 256.to_u8!).should eq EXPECTED_ADDPOS
    end

    it "should apply ADDPOS, XORPOS correctly" do
      ph = create_obj
      encode = ph.apply_ciphers([create_cipher_addpos, create_cipher_xorpos], HELLO_WORLD, 0)
      decode = ph.apply_ciphers([create_cipher_xorpos, create_cipher_addpos], encode, 0, is_decode: true)
      decode.should eq HELLO_WORLD
    end
  end
end
