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
    require 'metriks'
    @random_key_prefix = SecureRandom.hex
    @offset_counter = ThreadSafe::Cache.new { |h,k| h[k] = Metriks.counter( offset_key(k)) }
    @had_events = true 
    @last_had_events = true
  end # def register

  public
  def filter(event)
    return unless filter?(event) 
    if event["offset"].nil? || event[@offset_indicator].nil? || event["msg_len"].nil?
      raise Exception.new("Bad log file, #{event}, #{event['offset']}")
      return
    end
    @had_events = true
    offset_diff = (event['offset'] + event["msg_len"]) - @offset_counter[event[@offset_indicator]].count
    if offset_diff > 0
      @offset_counter[event[@offset_indicator]].increment(offset_diff)
    end
  end # def filter

  public
  def flush(options = {})
    if @had_events
      puts 'had messages'
      @last_had_events = true
      @had_events = false
    else
      @last_had_events = false
      event = LogStash::Event.new
      event["message"] = "No msgs being recieved from input. Most likely safe to shutdown"
      filter_matched(event)
      puts 'no messages for a while!'
    end
    return
  end # flush
  
  public
  def periodic_flush
    true
  end # periodic_flush

  private
  def offset_key(key)
    "#{@random_key_prefix}_#{key}"
  end # def metric_key 
  
  private
  def write_offsets
    open(@offset_path, 'a') do |f|
      @offset_counter.each_pair do |path, counter|
        f.puts "#{path}:#{counter.count}"
      end
    end
  end # write_offsets

  def teardown
    if @offset_path != ""
      if File.exist?(@offset_path)
        File.delete(@offset_path)
      end
      if !@had_events && !@last_had_events
        write_offsets
      end
      @offset_path = ""
    end
  end # def teardown
end # class LogStash::Filters::Example
