SEQUENCE_MAX = (2**32-1)

class SequenceGenerator:
  _sequence = 0
  
  def next(self):
    self._sequence = self._sequence + 1
    if self._sequence > SEQUENCE_MAX:
      self._sequence = 1
    return self._sequence
