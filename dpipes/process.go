package dpipes

import (
	"errors"
)

// Pipeline constructs pipelines out of processors.
func Pipeline(stages ...Process) Process {
	if len(stages) < 1 {
		panic(errors.New("must give at least one pipeline stage"))
	}
	if len(stages) == 1 {
		return stages[0]
	}
	return func(inch Pipe, outch Pipe) {
		for i, f := range stages {
			temp := make(Pipe, 100)
			if i == len(stages)-1 {
				temp = outch
			}
			go f(inch, temp)
			inch = temp
		}
	}
}

// Processing takes samples from source, process them with the
// given process (which might be a Pipeline), and writes them to
// the given sink.
func Processing(source func(Pipe), process Process, sink func(Pipe)) {
	inch := make(Pipe, 100)
	outch := make(Pipe, 100)
	go func() {
		Debug.Println("Processing start source")
		source(inch)
		Debug.Println("Processing finished source")
	}()
	if process != nil {
		go func() {
			Debug.Println("processing start process")
			process(inch, outch)
			Debug.Println("processing finished process")
		}()
	} else {
		outch = inch
	}
	Debug.Println("processing start sink")
	sink(outch)
	Debug.Println("processing finished sink")
}
