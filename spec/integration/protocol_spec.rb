require 'spec_helper'

require 'ione/rpc'

describe 'Low level protocol interaction' do
  before :all do
    @controller = Kafkactl.new
    @controller.stop
    @controller.clear
    @controller.start
    @topic_counter = [0]
  end

  after :all do
    @controller.stop
  end

  let :client do
    codec = Stern::Protocol::KafkaCodec.new
    Ione::Rpc::Client.new(codec, hosts: %w[localhost:9192]).start.value
  end

  let :test_sequence_number do
    (@topic_counter[0] += 1)
  end

  let :topic_name do
    'stern.test.topic.%d' % test_sequence_number
  end

  let :partition_id do
    3
  end

  def load_topic_metadata(broker=nil)
    request = Stern::Protocol::MetadataRequest.new(nil, [topic_name])
    request = broker ? Stern::Protocol::AddressedRequest.new(request, broker) : request
    10.times do
      response_bytes = client.send_request(request).value
      response = Stern::Protocol::MetadataResponse.decode(response_bytes)
      topic_metadata = response.topic_metadata[topic_name]
      if topic_metadata.error? || topic_metadata.partition_metadata[partition_id].error?
        sleep(1)
      else
        return response
      end
    end
    fail('Topic not created in 10 attempts')
  end

  def ensure_connected
    response = load_topic_metadata
    topic_metadata = response.topic_metadata[topic_name]
    partition_metadata = topic_metadata.partition_metadata[partition_id]
    leader = partition_metadata.leader
    client.add_host(leader.host, leader.port).value
    load_topic_metadata(leader)
    leader
  end

  def publish_message(key, value)
    partition_leader = ensure_connected
    message_set = Stern::Protocol::MessageSet.new([Stern::Protocol::Message.new(key, value)])
    topic_message_sets = {topic_name => {partition_id => message_set}}
    request = Stern::Protocol::ProduceRequest.new(nil, 1, 1000, topic_message_sets)
    request = Stern::Protocol::AddressedRequest.new(request, partition_leader)
    10.times do
      response_bytes = client.send_request(request).value
      response = Stern::Protocol::ProduceResponse.decode(response_bytes)
      message_offset = response.topic_offsets[topic_name].partition_offsets[partition_id]
      if message_offset.error?
        sleep(1)
      else
        return message_offset
      end
    end
    fail('Could not publish message in 10 attempts')
  end

  def load_first_offset
    partition_leader = ensure_connected
    queries = [Stern::Protocol::OffsetRequest::Query.new(topic_name, partition_id, -2, 1)]
    request = Stern::Protocol::OffsetRequest.new(nil, -1, queries)
    request = Stern::Protocol::AddressedRequest.new(request, partition_leader)
    10.times do
      response_bytes = client.send_request(request).value
      response = Stern::Protocol::OffsetResponse.decode(response_bytes)
      partiton_offsets = response.topic_offsets[topic_name].partition_offsets[partition_id]
      if partiton_offsets.error?
        sleep(1)
      else
        return partiton_offsets.offsets.first
      end
    end
    fail('Could not load the partiton offset in 10 attempts')
  end

  def consume_message
    partition_leader = ensure_connected
    offset = load_first_offset
    fetches = [Stern::Protocol::FetchRequest::Fetch.new(topic_name, partition_id, offset, 2048)]
    request = Stern::Protocol::FetchRequest.new(nil, -1, 1000, 1024, fetches)
    request = Stern::Protocol::AddressedRequest.new(request, partition_leader)
    10.times do
      response_bytes = client.send_request(request).value
      response = Stern::Protocol::FetchResponse.decode(response_bytes)
      message_set = response.topic_fetches[topic_name].partition_fetches[partition_id].message_set
      if message_set.nil? || message_set.empty?
        sleep(1)
      else
        return message_set
      end
    end
    fail('Could not fetch message in 10 attempts')
  end

  after do
    client.stop.value
  end

  it 'creates a topic by asking for its metadata' do
    response = load_topic_metadata
    topic_metadata = response.topic_metadata[topic_name]
    expect(topic_metadata.partition_metadata.keys).to contain_exactly(0, 1, 2, 3, 4, 5)
  end

  it 'publishes a message to a topic' do
    publish_message('foo', 'bar')
  end

  it 'consumes a message from a topic' do
    publish_message('foo', 'bar')
    message_set = consume_message
    message = message_set.to_a.last
    expect(message.key).to eq('foo')
    expect(message.value).to eq('bar')
  end
end
