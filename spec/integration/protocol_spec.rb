require 'spec_helper'

require 'ione/rpc'

describe 'Low level protocol interaction' do
  before :all do
    @controller = Kafkactl.new
    @controller.stop
    @controller.clear
    @controller.start
    @topic_counter = 0
  end

  after :all do
    @controller.stop
  end

  let :client do
    codec = Stern::Protocol::KafkaCodec.new
    Ione::Rpc::Client.new(codec, hosts: %w[localhost:9192]).start.value
  end

  let :topic_name do
    'stern.test.topic.%d' % (@topic_counter += 1)
  end

  let :partition_id do
    3
  end

  def load_topic_metadata
    10.times do
      metadata_request = Stern::Protocol::MetadataRequest.new(nil, [topic_name])
      response_bytes = client.send_request(metadata_request).value
      response = Stern::Protocol::MetadataResponse.decode(response_bytes)
      topic_metadata = response.topic_metadata[topic_name]
      if topic_metadata.error?
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
    client.add_host(partition_metadata.leader.host, partition_metadata.leader.port).value
    partition_metadata.leader
  end

  def publish_message(key, value)
    partition_leader = ensure_connected
    message_set = Stern::Protocol::MessageSet.new([Stern::Protocol::Message.new(key, value)])
    topic_message_sets = {topic_name => {partition_id => message_set}}
    request = Stern::Protocol::ProduceRequest.new(nil, 1, 1000, topic_message_sets)
    request = Stern::Protocol::AddressedRequest.new(request, partition_leader)
    response_bytes = client.send_request(request).value
    Stern::Protocol::ProduceResponse.decode(response_bytes)
  end

  def load_first_offset
    partition_leader = ensure_connected
    queries = [Stern::Protocol::OffsetRequest::Query.new(topic_name, partition_id, -2, 1)]
    request = Stern::Protocol::OffsetRequest.new(nil, -1, queries)
    request = Stern::Protocol::AddressedRequest.new(request, partition_leader)
    response_bytes = client.send_request(request).value
    response = Stern::Protocol::OffsetResponse.decode(response_bytes)
    response.topic_offsets[topic_name].partition_offsets[partition_id].offsets.first
  end

  def consume_message
    partition_leader = ensure_connected
    offset = load_first_offset
    fetches = [Stern::Protocol::FetchRequest::Fetch.new(topic_name, partition_id, offset, 2048)]
    request = Stern::Protocol::FetchRequest.new(nil, -1, 1000, 1024, fetches)
    request = Stern::Protocol::AddressedRequest.new(request, partition_leader)
    response_bytes = client.send_request(request).value
    response = Stern::Protocol::FetchResponse.decode(response_bytes)
    response.topic_fetches[topic_name].partition_fetches[partition_id].message_set
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
    response = publish_message('foo', 'bar')
    partition_offset = response.topic_offsets[topic_name].partition_offsets[3]
    expect(partition_offset).not_to be_error
  end

  it 'consumes a message from a topic' do
    response = publish_message('foo', 'bar')
    message_set = consume_message
    message = message_set.to_a.last
    expect(message.key).to eq('foo')
    expect(message.value).to eq('bar')
  end
end
