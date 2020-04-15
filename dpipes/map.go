package dpipes

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

type MultiSampleF func(Sample) ([]Sample, error)

// MapSamples maps a SampleF over a Pipe
func MultiMapSamples(f MultiSampleF, ignoreerrs bool) Process {
	return func(inch Pipe, outch Pipe) {
		for sample := range inch {
			nsamples, err := f(sample)
			if err != nil {
				if ignoreerrs {
					continue
				} else {
					panic(err)
				}
			}
			for _, nsample := range nsamples {
				outch <- nsample
			}
		}
		close(outch)
	}
}
