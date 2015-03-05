require 'spec_helper'

module Stern
  module Protocol
    describe Broker do
      describe '#eql?' do
        it 'is equal to another object with the same properties' do
          b1 = described_class.new(1, 'one', 1)
          b2 = described_class.new(1, 'one', 1)
          expect(b1).to eql(b2)
        end

        it 'is aliased as #==' do
          b1 = described_class.new(1, 'one', 1)
          b2 = described_class.new(1, 'one', 1)
          expect(b1).to eq(b2)
        end

        it 'is not equal to nil' do
          b1 = described_class.new(1, 'one', 1)
          expect(b1).not_to eql(nil)
        end

        it 'is not equal to an object with a different node ID' do
          b1 = described_class.new(1, 'one', 1)
          b2 = described_class.new(2, 'one', 1)
          expect(b1).not_to eql(b2)
        end

        it 'is not equal to an object with a different host' do
          b1 = described_class.new(1, 'one', 1)
          b2 = described_class.new(1, 'two', 1)
          expect(b1).not_to eql(b2)
        end

        it 'is not equal to an object with a different port' do
          b1 = described_class.new(1, 'one', 1)
          b2 = described_class.new(1, 'one', 2)
          expect(b1).not_to eql(b2)
        end
      end

      describe '#hash' do
        it 'is the same when the properties are the same' do
          b1 = described_class.new(1, 'one', 1)
          b2 = described_class.new(1, 'one', 1)
          expect(b1.hash).to eq(b2.hash)
        end

        it 'is not the same when the node ID is different' do
          b1 = described_class.new(1, 'one', 1)
          b2 = described_class.new(2, 'one', 1)
          expect(b1.hash).not_to eq(b2.hash)
        end

        it 'is not the same when the host is different' do
          b1 = described_class.new(1, 'one', 1)
          b2 = described_class.new(1, 'two', 1)
          expect(b1.hash).not_to eq(b2.hash)
        end

        it 'is not the same when the port is different' do
          b1 = described_class.new(1, 'one', 1)
          b2 = described_class.new(1, 'one', 2)
          expect(b1.hash).not_to eq(b2.hash)
        end
      end
    end
  end
end
