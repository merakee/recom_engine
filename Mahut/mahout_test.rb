#!/usr/bin/env ruby -w
# -*- coding: utf-8 -*-

#  Test code for Mahut recommendation engines. 
#  Bijit Halder
#  Created: 16 Aug 2014  
#  Revision History: 
#
require 'csv'

Dir.glob("#{ENV['MAHOUT_DIR']}/libexec/*.jar").each { |d| require d }

# define all vairables
$model =  nil
$similarity =  nil
$neighborhood =  nil
$recommender =  nil
$threshold =3.0
$data_file_name = "ratings_data_sr_90_bool.csv"
$test_file_name = "ratings_test_sr_90_bool.csv"
$csv_t = nil 
$similarity_types =%w[Euclidean Pearson UncenteredCosine Spearman LogLikelihood Tanimoto]
$neighborhood_sizes=[50,100,500,1000]
$recommender_types =%w[GenericUserBased GenericBooleanPrefUserBased]
$factorizer = nil
$factorizer_types =%w[ALSWRFactorizer SVDPlusPlusFactorizer ParallelSGDFactorizer  RatingSGDFactorizer]
$lamda_vals =[0.1, 0.05, 0.01]
$num_featues_vals =[50, 100, 200, 500]
$num_iterations_vals =[5, 10, 20]
$recommender_svd_types =%w[SVDRecommender]
$user_ids =[4169,1]

$logfile = open('performance.log', 'a')

# Mahut
MahoutFile = org.apache.mahout.cf.taste.impl.model.file
MahoutSimilarity = org.apache.mahout.cf.taste.impl.similarity
MahoutNeighborhood = org.apache.mahout.cf.taste.impl.neighborhood
MahoutRecommender = org.apache.mahout.cf.taste.impl.recommender
MahoutRecommenderSVD = org.apache.mahout.cf.taste.impl.recommender.svd

def set_model(filename=$data_file_name)
	$model = MahoutFile.FileDataModel.new(java.io.File.new(filename))
end

def set_similatiry(type=$similarity_types[0])
	$similarity = case type 
	when "Pearson"
		MahoutSimilarity.PearsonCorrelationSimilarity.new($model)
	when "UncenteredCosine"
		MahoutSimilarity.UncenteredCosineSimilarity.new($model)
	when "Spearman"	
		MahoutSimilarity.SpearmanCorrelationSimilarity.new($model)
	when "LogLikelihood"
		MahoutSimilarity.LogLikelihoodSimilarity.new($model)
	when "Tanimoto" 
		MahoutSimilarity.TanimotoCoefficientSimilarity.new($model)
	else 
		MahoutSimilarity.EuclideanDistanceSimilarity.new($model)
	end
end

def set_neighborhood(neighborhood_size=$neighborhood_sizes[0])
	$neighborhood = MahoutNeighborhood.NearestNUserNeighborhood.new(neighborhood_size, $similarity, $model)
end

def set_recommender(type=$recommender_types[0])
	$recommender = case type 
	when "GenericBooleanPrefUserBased"
		MahoutRecommender.GenericBooleanPrefUserBasedRecommender.new($model, $neighborhood, $similarity)
	else
		MahoutRecommender.GenericUserBasedRecommender.new($model, $neighborhood, $similarity)
	end
end

def get_recommendation(user_id=$user_ids[0],recommendations_size=10)
	$recommender.recommend(user_id, recommendations_size)
end

def get_estimate(user_id,item_id)
	$recommender.estimatePreference(user_id,item_id)
end

def check_estimate(user_id=$user_ids[0],recommendations_size=10)
	for rec in get_recommendation(user_id,recommendations_size)
		item_id = rec.get_item_id
		etimates = $recommender.estimatePreference(user_id,item_id)
		etimates_b = (etimates > $threshold)?5:1
		puts "#{rec}: \t raw estimate: #{etimates} \t bool estimate: #{etimates_b}"
	end
end

def model_info(model)
	puts "Model Info............................"
	puts "Max Pref val: #{model.getMaxPreference()}"   
	puts "Min Pref val: #{model.getMinPreference()}"
           
	puts "Number of items: #{model.getNumItems()}"  	      
	puts "Number of users: #{model.getNumUsers()}"      
end

def open_csv_file(file_name)
    $csv_t  = CSV.open(file_name, 'w' ) 
end

def convert_to_bool(estimate)
	(estimate > $threshold)?5.0:1.0
end

def cal_error(true_val,estimate)
	estimate_b= convert_to_bool(estimate)
	error = true_val != estimate_b
	puts [true_val, estimate_b] if error  && !([1.0,5.0].include?(estimate_b))
	error 
end

def calculate_performance(fixed_count=0)
	total_count=0
	error_count = 0
	CSV.foreach($test_file_name) do |row|
		row=row.collect{|v| v.to_f}
		estimate = get_estimate(row[0],row[1])
		total_count +=1
		error_count +=1 if cal_error(row[2],estimate)
		#puts [ row[2] , estimate, cal_error(row[2],estimate)]
		if total_count % 10000 == 0 
			pcorrect = 1.0-(error_count.to_f/total_count)
			log_text "Total: #{total_count} \t Error Count: #{error_count} \t %Correct: #{pcorrect}"
		end
		break if total_count==fixed_count
	end
	pcorrect = 1.0-(error_count.to_f/total_count)
	log_text "Total: #{total_count} \t Error Count: #{error_count} \t %Correct: #{pcorrect}"
	log_text "--------------------------------------------"
end

def log_text(text)
	puts text
	$logfile.puts text 
end

def run_all(test_size)
	for simtype in $similarity_types
		for nsize in $neighborhood_sizes
			for rcomtype in [$recommender_types[0]]
				run_test(simtype,nsize,rcomtype,test_size)
			end
		end
	end
end

def run_test(simtype,nsize,rcomtype,test_size)
	set_similatiry(simtype)
	set_neighborhood(nsize)	
	set_recommender(rcomtype)
	log_text "Data file: #{$data_file_name}\tSimilarity type: #{simtype}\tNeighborhood size: #{nsize}\t Recommender type: #{rcomtype}\t Test Size: #{test_size}"
	log_text "--------------------------------------------"
	calculate_performance(test_size)
end


def set_factorizer(factype,numFeatures,lambda,numIterations)
	$factorizer = case factype
	when "SVDPlusPlusFactorizer"
		MahoutRecommenderSVD.SVDPlusPlusFactorizer.new($model,numFeatures,numIterations)
	else
		MahoutRecommenderSVD.ALSWRFactorizer.new($model,numFeatures,lambda,numIterations)
	end

end

def set_recommender_svd(type=$recommender_svd_types[0])
	$recommender = case type 
	when "SVDRecommender"
		MahoutRecommenderSVD.SVDRecommender.new($model,$factorizer)
	else
		MahoutRecommenderSVD.SVDRecommender.new($model,$factorizer)
	end
end

def run_sdv_all(factype,test_size)
	for numFeatures in $num_featues_vals
		for lambda in $lamda_vals
			for numIterations in $num_iterations_vals
				run_svd_test(factype,numFeatures,lambda,numIterations,test_size)
			end
		end
	end
end

def run_svd_test(factype,numFeatures,lambda,numIterations,test_size)
	rcomtype = $recommender_svd_types[0]
	set_factorizer(factype,numFeatures,lambda,numIterations)
	set_recommender_svd($recommender_svd_types[0])
	log_text "Data file: #{$data_file_name}\tRecommender type: #{rcomtype}\tFactorization Type: #{factype} \t Num Features type: #{numFeatures}\tLambda: #{lambda}\tNum Of iteration: #{numIterations}\tTest Size: #{test_size}"
	log_text "--------------------------------------------"
	calculate_performance(test_size)
end

# set everything
set_model
#run_test($similarity_types[4],$neighborhood_sizes[2],$recommender_types[0],100000)
#run_svd_test($factorizer_types[0],50,0.2,20,0)
run_svd_test($factorizer_types[1],10,0.2,2,0)
#run_sdv_all($factorizer_types[0],10000)
