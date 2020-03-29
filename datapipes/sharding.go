package datapipes

// CombinePipes takes a channel of Pipes and combines
// them into a single output stream.
func CombinePipes(inches chan Pipe, outch Pipe) {
	Debug.Println("CombinePipes")
	for inch := range inches {
		Debug.Println(inch)
		for sample := range inch {
			outch <- sample
		}
		Debug.Println(inch, "done")
	}
	Debug.Println("CombinePipes closing")
	close(outch)
}

// MakeShards splits up a stream of samples by size and count and
// outputs channels for each shard on the output channel.
func MakeShards(maxcount, maxsize int) func(Pipe, chan Pipe) {
	Assert(maxcount >= 2, "maxcount too small")
	Assert(maxsize >= 1000, "maxsize too small")
	return func(inch Pipe, outch chan Pipe) {
		var current Pipe = nil
		size := 0
		count := 0
		for sample := range inch {
			if count >= maxcount || size >= maxsize {
				if current != nil {
					close(current)
				}
				current = nil
				count = 0
				size = 0
			}
			if current == nil {
				current = make(Pipe)
				outch <- current
			}
			current <- sample
			count++
			size += SampleSize(sample)
		}
		close(current)
		close(outch)
	}
}
