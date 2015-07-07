# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "securerandom"
# This example filter will replace the contents of the default 
# message field with whatever you specify in the configuration.
#
# It is only intended to be used as an example.
class LogStash::Filters::Recreateoffsets < LogStash::Filters::Base

  # Setting the config_name here is required. This is how you
  # configure this filter from your Logstash config.
  #
  # filter {
  #   example {
  #     message => "My message..."
  #   }
  # }
  #
  config_name "recreateoffsets"

  # The event attribute that indicates which offset group the message
  # is from   
  config :offset_indicator, :validate => :string, :default => ''
 
  # The file in which the offsets will be saved into
  config :offset_path, :validate => :string, :default => ''

  public
  def register
    require 'thread_safe'
    require 'atomic'
    @random_key_prefix = SecureRandom.hex
    @offset_counter = ThreadSafe::Cache.new
  end # def register

  public
  def filter(event)
    return unless filter?(event) 
    if @offset_counter[event[@offset_indicator]].nil?  
      @offset_counter[event[@offset_indicator]] = Atomic.new(event['offset'].to_i)
    end
    @offset_counter[event[@offset_indicator]].update { |v| [v, event['offset'].to_i].max } 
    # filter_matched should go in the last line of our successful code
  end # def filter
  def flush(options = {})
    @offset_counter.each_pair do |k,v|
      puts "#{k},#{v.value}"
    end 
    event = LogStash::Event.new
    filter_matched(event)
  end # flush
  def periodic_flush
    true
  end # periodic_flush
  def metric_key(key)
    "#{@random_key_prefix}_#{key}"
  end # def metric_key 
end # class LogStash::Filters::Example
