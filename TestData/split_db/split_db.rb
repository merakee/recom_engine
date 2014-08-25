#!/usr/bin/env ruby -w
# -*- coding: utf-8 -*-

# Purpose:: Clean up DB and split into two csv files 
#
#
#
# Coptyright:: Indriam Inc.
# Created By:: Bijit Halder
# Created on:: 28 April 2010
# Last Modified:: 20 July 2014
# Modification History::
#
#
#

# Database set up
require 'set'
require 'csv'
require '../sqlite_api.rb'

#====================================
# get dir name
# if(ARGV.length==0)
# 	puts "\n **Error ***: There is no csv file specified. Please specify a file. \n\n"
# 	Process.exit
# end
$split_ratio = 0.9
tag  = "_sr_#{($split_ratio*100).to_i}"
$csv_filename_d = "ratings_data#{tag}.csv" #ARGV[0]
$csv_filename_t = "ratings_test#{tag}.csv" #ARGV[0]
$db =  nil
$table_name =  nil 
$min_count = 4;
$cvs_t =nil
$csv_d = nil 
$user_t = Set.new
$user_d = Set.new 

def open_db
	$db =  SqliteApi.new("MLRatings.sqlite")
	$table_name = "ratings_table"
end

def clean_db
	# delete all contents that have less than min_count ratings
	sql = "DELETE FROM #{$table_name} WHERE content_id IN (SELECT content_id FROM #{$table_name} GROUP BY content_id HAVING COUNT(*) < #{$min_count})"
	puts $db.execute_sql(sql)
	# delete all users that have less than min_count ratings
	sql = "DELETE FROM #{$table_name} WHERE user_id IN (SELECT user_id FROM #{$table_name} GROUP BY user_id HAVING COUNT(*) < #{$min_count})"
	puts $db.execute_sql(sql)
	sql = "VACUUM"
	$db.execute_sql(sql)

end

def open_files
	$csv_d  = CSV.open($csv_filename_d, 'w' ) 
	$csv_t  = CSV.open($csv_filename_t, 'w' ) 
end

def write_to_csv_d(val)
    $csv_d << val 
end
def write_to_csv_t(val)
    $csv_t << val 
end

def randomize_split_and_add(vals)
	vals.shuffle!
	vcount = vals.size
	vcount_h = (vcount*$split_ratio).ceil
	(0..vcount_h-1).each{|ind|
		write_to_csv_d vals[ind]
		# add to user_d set
		$user_d << vals[ind][0]
	}
	(vcount_h..vcount-1).each{|ind|
		write_to_csv_t vals[ind]
		# add to user_t set
		$user_t << vals[ind][0]
	}
end

def add_data
	sql  = "SELECT DISTINCT content_id FROM #{$table_name}";
	content_ids =  $db.execute_sql(sql)
	content_ids.each{|content_id|
			#content_id=[33]
			sql  = "SELECT user_id, content_id, ratings FROM #{$table_name} WHERE content_id = #{content_id[0]}";
			randomize_split_and_add $db.execute_sql(sql)
			#break  
	}
end

def check_split
	if $user_t == $user_d 
		puts "No empty user......" 
	else 
		puts "Empty user....."
	end

end

open_db
#clean_db 
# CSV.foreach(csv_filename) do |row|
# 	val = row[0].split("::")[0,3]
# 	insert_data(user_id: val[0],content_id:val[1],ratings:val[2])
# end
open_files
add_data
check_split
$db.close_db