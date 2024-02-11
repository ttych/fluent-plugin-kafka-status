# frozen_string_literal: true

# extensions for Time class
#   provides formater
#   - to epoch millisecond format
#   - to iso format with millisecond
class Time
  def to_epochmillis
    (to_f * 1000).to_i
  end

  def to_iso
    iso8601(3)
  end
end
