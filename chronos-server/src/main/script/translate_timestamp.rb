#!/usr/bin/env ruby
#-*- coding: utf-8 -*-

def usage
  puts "Usage: ruby translate_timestamp.rb $timestamp"
end

# Translate chronos timestamp into readable world time.
def translate_timestamp(timestamp)
  Time.at((timestamp >> 18) / 1000)
end

if __FILE__ == $0
  if ARGV.length == 1 and ARGV.first =~ /\d+/
    timestamp = ARGV.first.to_i
    puts translate_timestamp(timestamp)
  else
    puts "Wrong parameters, please try again."
    usage
  end
end

