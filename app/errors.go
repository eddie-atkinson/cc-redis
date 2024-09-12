package main

type EndOfBuffer struct{}

func (e EndOfBuffer) Error() string {
	return "Reached end of buffer without finding terminating \r\n string"
}

type InvalidTypeCoercion struct{}

// TODO: do some cool reflection to get the types her
func (e InvalidTypeCoercion) Error() string {
	return "Invalid to convert from X to Y"
}
