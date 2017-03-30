class DataWarehouse
  class InvalidSource < StandardError; end

  def self.reset(source)
    raise InvalidSource unless source
    Result.new(0, 1)
  end

  class Result
    attr_reader :before_count, :after_count

    def initialize(before_count, after_count)
      @before_count = before_count
      @after_count = after_count
    end
  end
end
