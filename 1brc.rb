require "benchmark"

class NaiveBillionRowChallenge
  def self.call
    new.call
  end

  def initialize
    @filename = "measurements-100.txt"
    @results = {}
  end

  def call
    # Hamburg;12.0
    # Bulawayo;8.9
    File.foreach(@filename) do |line|
      city, temp = line.split(";")
      temp = temp.to_f

      @results[city] ||= { min: nil, mean: nil, max: nil, sum: 0, count: 0 }
      data = @results[city]

      begin
        data[:min] = temp if data[:min].nil? || temp < data[:min]
        data[:max] = temp if data[:max].nil? || temp > data[:max]
        data[:count] += 1
        data[:sum] += temp
        data[:mean] = (data[:sum] / data[:count]).round(1)
      rescue
        # require 'pry'
        # binding.pry
        # exit
      end

      @results[city] = data
    end

    @results.each do |city, data|
      puts "#{city}=#{data[:min]}/#{data[:mean]}/#{data[:max]}"
    end
    # output min/mean/max of each station to STDOUT
  end

  private
end

time = Benchmark.measure do
  NaiveBillionRowChallenge.call
end

puts "Completed in #{time} seconds"
