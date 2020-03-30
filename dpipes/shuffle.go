package dpipes

import "math/rand"

// Shuffle samples using an in-memory shuffle buffer
func Shuffle(bufsize, minsize int) Process {
	return func(inch Pipe, outch Pipe) {
		if minsize > bufsize {
			minsize = bufsize
		}
		buffer := make([]Sample, 0, bufsize)
		for {
			// Debug.Println("Shuffle", len(buffer))
			if len(buffer) < bufsize {
				sample, ok := <-inch
				if !ok {
					break
				}
				buffer = append(buffer, sample)
			}
			if len(buffer) < minsize {
				continue
			}
			sample, ok := <-inch
			if !ok {
				break
			}
			index := rand.Int() % len(buffer)
			sample, buffer[index] = buffer[index], sample
			outch <- sample
		}
		for len(buffer) > 0 {
			// Debug.Println("Shuffle+", len(buffer))
			index := rand.Int() % len(buffer)
			sample := buffer[index]
			if index != len(buffer)-1 {
				buffer[index] = buffer[len(buffer)-1]
			}
			buffer = buffer[:len(buffer)-1]
			outch <- sample
		}
		close(outch)
		Debug.Println("Shuffle", "done")
	}
}
