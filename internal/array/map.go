package array

func Map[T, U any](data []T, f func(T) U) []U {
	res := make([]U, len(data))

	for i, v := range data {
		res[i] = f(v)
	}
	return res
}
