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

      data[:min] = temp if data[:min].nil? || temp < data[:min]
      data[:max] = temp if data[:max].nil? || temp > data[:max]
      data[:count] += 1
      data[:sum] += temp

      @results[city] = data
    end

    @results.each do |city, data|
      mean = (data[:sum] / data[:count]).round(1)
      puts "#{city}=#{data[:min]}/#{mean}/#{data[:max]}"
    end
  end
end

NaiveBillionRowChallenge.call
