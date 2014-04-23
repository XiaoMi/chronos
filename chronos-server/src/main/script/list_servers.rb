#!/usr/bin/env ruby
#-*- coding: utf-8 -*-

require "zk"

def usage
  puts "Usage: ruby list_servers.rb $zkQuorum $clusterName"
end

# Tool to print running chronos servers and allocated timestamp.
class TimestampServerUtil

  def list_servers(zkQuorum, clusterName)
    base_znode = "/chronos/" + clusterName
    master_znode = base_znode + "/master"
    backup_servers_znode = base_znode + "/backup-servers"
    persistent_timestamp_znode = base_znode + "/persistent-timestamp"
    
    begin
      zk = ZK.new(zkQuorum)
      puts "Connected ZooKeeper #{zkQuorum}"
      
      if zk.exists?(base_znode)

        # print master server
        if zk.exists?(master_znode)
          master = zk.get(master_znode)[0]
          puts "Master is #{master}"
        else
          puts "No master is running"
        end

        # print backup masters
        backup_servers = zk.children(backup_servers_znode)
        if backup_servers.empty?
          puts "No backup master is running"
        else
          backup_servers.each do |backup_server|
            puts "Backup server is #{backup_server}"
          end
        end

        # print persistent timestamp
        if zk.exists?(persistent_timestamp_znode)
          persistent_timestamp_bytes = zk.get(persistent_timestamp_znode)[0]
          l = 0 # bytes to long
          persistent_timestamp_bytes.each_byte do |byte|
            l = l << 8
            l ^= byte & 0xff
          end
          persistent_timestamp = l
          datetime = Time.at((l >> 18) / 1000) # readable world time
          puts "Persistent-timestamp is #{persistent_timestamp} (#{datetime})"
        else
          puts "#{persistent_timestamp_znode} doesn't exit"
        end
        
      else
        puts "#{base_znode} doesn't exist, exit immediately"
        return
      end
    end

  rescue
    puts "Error to list #{clusterName} chronos servers with ZooKeeper #{zkQuorum}"
    return
  end
end

if __FILE__ == $0
  if ARGV.length == 2
    zkQuorum = ARGV[0]
    clusterName = ARGV[1]
    TimestampServerUtil.new.list_servers(zkQuorum, clusterName)
  else
    puts "Wrong parameters, please try again."
    usage
  end
end

