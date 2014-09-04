#!/usr/bin/env ruby -w
# -*- coding: utf-8 -*-

#  Test code for Mahut recommendation engines. 
#  Bijit Halder
#  Created: 16 Aug 2014  
#  Revision History: 
#
require 'csv'
require 'benchmark'

Dir.glob("#{ENV['MAHOUT_DIR']}/libexec/*.jar").each { |d| require d }

# define all vairables
$data_file_name = "ratings_data_bool.csv"
$$data_file_name= "ratings_data_sr_90.csv"
#$factorizer_types =%w[ALSWRFactorizer SVDPlusPlusFactorizer ParallelSGDFactorizer  RatingSGDFactorizer]

# Mahut
MahoutFile = org.apache.mahout.cf.taste.impl.model.file
MahoutRecommender = org.apache.mahout.cf.taste.impl.recommender
MahoutRecommenderSVD = org.apache.mahout.cf.taste.impl.recommender.svd

def run_svd_test(numFeatures,numIterations)
	$model = MahoutFile.FileDataModel.new(java.io.File.new($data_file_name))
	$factorizer=MahoutRecommenderSVD.SVDPlusPlusFactorizer.new($model,numFeatures,numIterations)
	pfile  = java.io.File.new("svd_data.data")
	$persistency_policy = MahoutRecommenderSVD.FilePersistenceStrategy.new(pfile)
	$recommender = 	MahoutRecommenderSVD.SVDRecommender.new($model,$factorizer,$persistency_policy)	
end

# set everything
bench_mark = Benchmark.bm(7) do |item|
	item.report("svd: ") {run_svd_test(50,2)}
end
