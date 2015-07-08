# Logstash Plugin Filter - Recreate Offsets

This is an input plugin for [Logstash](https://github.com/elasticsearch/logstash).

This plugin part of the enhanced series of logstash plugins. It is meant to be used in the case of an ungraceful shutdown or error regarding the saved offset file.


## Documentation

To run this code one simply needs to specify the source in which the enhanced logstash messages were stored by the input plugin, and allow this plugin to read through all of the messages in that source.  

The key differences you should see in the config file are:
  - `offset_path` - where the offsets will be stored after shutdown. 
  - `offset_indicator` - the attribute in the event that indicates from where/who does the offset come from. 
