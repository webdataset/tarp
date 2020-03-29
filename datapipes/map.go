package datapipes

// MapSamples maps a SampleF over a Pipe
func MapSamples(f SampleF, ignoreerrs bool) Process {
	return func(inch Pipe, outch Pipe) {
		for sample := range inch {
			nsample, err := f(sample)
			if err != nil {
				if ignoreerrs {
					continue
				} else {
					panic(err)
				}
			}
			outch <- nsample
		}
		close(outch)
	}
}
