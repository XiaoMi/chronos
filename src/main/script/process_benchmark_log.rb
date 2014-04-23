#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

def usage
  puts "Usage: ruby process_benchmark_log.rb $file"
end

# Process log file, then print failover time, qps and latency.
def process_benchmark_log(file_name)
  total_failover_number = 0
  total_failover_time = 0
  total_qps_number = 0
  total_qps = 0
  total_latency = 0

  file = File.open file_name

  file.each do |line|
  
    if line =~ /After / # failover time
      total_failover_number += 1
      /failover time is (?<failover_time>\d+.?\d*) seconds/ =~ line
      total_failover_time += failover_time.to_f
    else # qps and latency
      total_qps_number += 1
      /qps: (?<qps>\d+.?\d*), latency: (?<latency>\d+.?\d*)ms/ =~ line
      total_qps += qps.to_f
      total_latency += latency.to_f
    end
    
  end
  
  file.close

  puts "Average failover time: #{total_failover_time/total_failover_number}"
  puts "Average qps: #{total_qps/total_qps_number}"
  puts "Average latency: #{total_latency/total_qps_number}"

end

if __FILE__ == $0
  if ARGV.length == 1
    file_name = ARGV.first
    process_benchmark_log(file_name)
  else
    puts "Wrong parameters, please try again."
    usage
  end
end

