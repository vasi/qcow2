package qcow2

func divceil(d int64, q int64) int64 {
	r := d / q
	if d%q != 0 {
		r++
	}
	return r
}
