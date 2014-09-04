#!/usr/bin/env ruby -w
# -*- coding: utf-8 -*-

#  Bijit Halder
#  Created: 16 Aug 2014  
#  Revision History: 
#

require 'csv'
$csv_filename = ""
$cvs_s  = nil 
$cvs_t  = nil 
$thershoold=3.5

def open_file
	file_name = $csv_filename.split(".")[0]+"_bool_35.csv"
    $csv_t  = CSV.open(file_name, 'w' ) 
end

def write_to_csv(val)
    $csv_t << val 
end

def convert_val(row)
	row[2]=row[2].to_f>$thershoold?5:1
	row 
end

# get file name 
if !ARGV[0]
	print "Enter File name: "
	$csv_filename = gets.chomp
else
	$csv_filename = ARGV[0]
end

open_file
CSV.foreach($csv_filename) do |row|
	write_to_csv(convert_val(row))
end
