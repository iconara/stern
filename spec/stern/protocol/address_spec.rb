require 'spec_helper'

module Stern
  module Protocol
    describe Address do
      describe '#eql?' do
        it 'is equal to another address with the same host and port' do
          a1 = described_class.new('example.com', 9092)
          a2 = described_class.new('example.com', 9092)
          expect(a1).to eql(a2)
        end

        it 'is aliased as #==' do
          a1 = described_class.new('example.com', 9092)
          a2 = described_class.new('example.com', 9092)
          expect(a1).to eq(a2)
        end

        it 'is not equal to nil' do
          a1 = described_class.new('example.com', 9092)
          expect(a1).not_to eql(nil)
        end

        it 'is not equal when the host is different' do
          a1 = described_class.new('example.com', 9092)
          a2 = described_class.new('example.org', 9092)
          expect(a1).not_to eql(a2)
        end

        it 'is not equal when the port is different' do
          a1 = described_class.new('example.com', 9092)
          a2 = described_class.new('example.com', 9292)
          expect(a1).not_to eql(a2)
        end
      end

      describe '#hash' do
        it 'is the same when the host and port are the same' do
          a1 = described_class.new('example.com', 9092)
          a2 = described_class.new('example.com', 9092)
          expect(a1.hash).to eql(a2.hash)
        end

        it 'is not equal when the host is different' do
          a1 = described_class.new('example.com', 9092)
          a2 = described_class.new('example.org', 9092)
          expect(a1.hash).not_to eql(a2.hash)
        end

        it 'is not equal when the port is different' do
          a1 = described_class.new('example.com', 9092)
          a2 = described_class.new('example.com', 9292)
          expect(a1.hash).not_to eql(a2.hash)
        end
      end
    end
  end
end
