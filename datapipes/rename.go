package datapipes

import "strings"

// RenameFields renames sample fields based on a list of specs
func RenameFields(specs []string) SampleF {
	return func(sample Sample) (Sample, error) {
		Debug.Println(specs)
		result := Sample{}
		for _, spec := range specs {
			names := strings.SplitN(spec, ":", 2)
			Debug.Println(names)
			after := names[0]
			before := names[0]
			if len(names) > 1 {
				before = names[1]
			}
			value, err := GetFirst(sample, before)
			Handle(err)
			result[after] = value
		}
		return result, nil
	}
}

// RenameSamples renames samples in a pipeline
func RenameSamples(specs []string, ignoreerrs bool) Process {
	return MapSamples(RenameFields(specs), ignoreerrs)
}
